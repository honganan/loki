package bbf

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/storage/chunk"
	"github.com/grafana/loki/v3/pkg/storage/config"
	"github.com/grafana/loki/v3/pkg/util/encoding"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	lokiring "github.com/grafana/loki/v3/pkg/util/ring"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/exp/slices"
)

const (
	WALFile        = "wal"
	MaxWalFileSize = 20 * 1024 * 1024

	defaultMonitorTimes = 60 * time.Second
)

type BBFComputer struct {
	services.Service
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	ctx    context.Context
	cancel context.CancelFunc

	bbfStore *BBFStore

	config     Config
	schema     config.SchemaConfig
	pool       *Pool
	stickyRing *StickyRing
	rm         *lokiring.RingManager

	wal *WAL

	ComputeChunksCounter prometheus.Counter
	UnreadOffsetGauge    prometheus.Gauge
}

func NewBBFComputer(
	store *BBFStore,
	cfg Config,
	periods []config.PeriodConfig,
	bbfIndexRM *lokiring.RingManager,
	reg prometheus.Registerer,
	logger log.Logger,
) (*BBFComputer, error) {
	ctx, cancel := context.WithCancel(context.Background())
	// 校验 wal 目录是否存在，不存在则创建
	if _, err := os.Stat(cfg.WalPath); os.IsNotExist(err) {
		if err := os.Mkdir(cfg.WalPath, 0755); err != nil {
			return nil, err
		}
	}

	wal, err := NewWAL(ctx, filepath.Join(cfg.WalPath, WALFile), MaxWalFileSize)
	if err != nil {
		return nil, err
	}

	sr, err := NewStickyRing(100, bbfIndexRM.Ring.KVClient, bbfIndexRM.Ring)
	if err != nil {
		return nil, err
	}

	factory := cfg.Factory
	if factory == nil {
		factory = PoolAddrFunc(func(addr string) (PoolClient, error) {
			return NewClient(cfg.ClientConfig, addr)
		})
	}
	pool := NewBBFClientPool("bbf-index", cfg.ClientConfig.PoolConfig, bbfIndexRM.Ring, factory, logger, "computer")

	c := &BBFComputer{
		ctx:      ctx,
		cancel:   cancel,
		bbfStore: store,
		wal:      wal,
		config:   cfg,
		schema: config.SchemaConfig{
			Configs: periods,
		},
		pool:       pool,
		stickyRing: sr,
		rm:         bbfIndexRM,

		ComputeChunksCounter: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "loki_bbf_compute_chunks_count",
			Help: "Total number of chunks computed by the BBF computer.",
		}),
		UnreadOffsetGauge: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "loki_bbf_unread_offset",
			Help: "The unread offset of the BBF computer WAL.",
		}),
	}

	go func() {
		err := wal.Read(c.config.ComputeChunksBatch, c.config.ComputeParallelism, c.compute)
		if err != nil {
			_ = level.Error(util_log.Logger).Log("msg", "computer consume wal failed", "err", err)
		}
	}()

	go func() {
		wal.monitorUnreadOffset(c.UnreadOffsetGauge)
	}()

	c.Service = services.NewBasicService(c.starting, c.running, c.stopping)
	return c, nil
}

func (c *BBFComputer) compute(ctx context.Context, chunks []string) {

	// 并发对 chunk 分词计算
	err := concurrency.ForEachJob(ctx, len(chunks), len(chunks), func(ctx context.Context, jobIndex int) error {

		chunkID := chunks[jobIndex]
		chk, err := parseChunkKey(chunkID)
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "error parsing chunk key", "chunkID", chunkID, "err", err)
			return err
		}

		entries, err := c.bbfStore.LoadChunk(context.Background(), chk)
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "error loading chunk", "chunkID", chunkID, "err", err)
			return err
		}

		t := getTokenizer()
		for entries.Next() {
			entry := entries.At()

			tokenizeString(t.m, entry.Line)
			// 当分词结果到 WriteBatchSize 的 85% 时，就发送到 BBF
			if len(t.m) >= c.config.WriteBatchSize*85/100 {
				err := c.writeToBBFWithRetries(ctx, t.m, chk)
				if err != nil {
					return err
				}
				t.reset()
			}
		}

		err = c.writeToBBFWithRetries(ctx, t.m, chk)
		if err != nil {
			return err
		}
		// putTokenizer 会 reset t
		putTokenizer(t)

		c.ComputeChunksCounter.Inc()

		return nil
	})
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "error computing", "err", err)
		return
	}
}

// writeToBBFWithRetries writes tokens to BBF with retries.
func (c *BBFComputer) writeToBBFWithRetries(ctx context.Context, tokens map[string]struct{}, chk chunk.Chunk) error {
	cfg := backoff.Config{
		MinBackoff: 2 * time.Second,
		MaxBackoff: 10 * time.Second,
		MaxRetries: 0,
	}
	retries := backoff.New(ctx, cfg)
	var lastErr error
	for retries.Ongoing() {
		lastErr = c.writeTokensToBBF(tokens, chk)
		if lastErr == nil {
			return nil
		}
		level.Warn(util_log.Logger).Log("msg", "error writing tokens to BBF, will retry it", "err", lastErr, "retry", retries.NumRetries())
		retries.Wait()
	}

	return lastErr
}

func (c *BBFComputer) writeTokensToBBF(tokens map[string]struct{}, chk chunk.Chunk) error {

	bucket := bloomBucket(chk.Through.Time())
	rs, err := c.stickyRing.StickyReplicationSetForOperation(ring.Read, bucket)
	if err != nil {
		return err
	}
	instances := rs.Instances
	slices.SortFunc(instances, func(i, j ring.InstanceDesc) int {
		return strings.Compare(i.Addr, j.Addr)
	})

	period, err := c.schema.SchemaForTime(chk.Through)
	if err != nil {
		return err
	}
	m := buildAddRequests(tokens, instances, period, chk.ChunkRef)

	for addr, req := range m {
		bbf, err := c.pool.GetClientFor(addr)
		if err != nil {
			return err
		}

		ctx := user.InjectOrgID(context.Background(), chk.UserID)
		_, err = bbf.(BBFClient).Add(ctx, req)
		if err != nil {
			return err
		}
	}

	return nil
}

func buildAddRequests(tokens map[string]struct{}, instances []ring.InstanceDesc, period config.PeriodConfig, ref logproto.ChunkRef) map[string]*AddRequest {
	m := make(map[string]*AddRequest, len(instances))
	logFlag := 0
	for token := range tokens {
		shard := int(encoding.XxhashHashIndex(StringToBytes(token), period.BBFShards))
		i := shard % len(instances)
		if logFlag == 0 {
			level.Debug(util_log.Logger).Log("msg", "shard", "shard", shard, "len(instance)", len(instances), "instance", instances[i].Addr)
			logFlag = 1
		}
		r, ok := m[instances[i].Addr]
		if !ok {
			r = &AddRequest{Records: make(map[int32]*AddRecord)}
			m[instances[i].Addr] = r
		}

		record, ok := r.Records[int32(shard)]
		if !ok {
			record = &AddRecord{
				Ref:    &ref,
				Shard:  int32(shard),
				Values: make([]string, 0, 100),
			}
			record.Values = append(record.Values, token)
			r.Records[int32(shard)] = record
		} else {
			record.Values = append(record.Values, token)
		}
	}
	return m
}

func parseChunkKey(chunkID string) (chunk.Chunk, error) {
	userIdx := strings.Index(chunkID, "/")
	if userIdx == -1 || userIdx+1 >= len(chunkID) {
		return chunk.Chunk{}, fmt.Errorf("invalid chunk ID: %s", chunkID)
	}
	userId := chunkID[:userIdx]

	chk, err := chunk.ParseExternalKey(userId, chunkID)
	if err != nil {
		return chunk.Chunk{}, fmt.Errorf("invalid chunk ID: %s", chunkID)
	}
	return chk, nil
}

func (c *BBFComputer) Compute(ctx context.Context, request *ComputeRequest) (*ComputeResponse, error) {
	if len(request.Refs) == 0 {
		return &ComputeResponse{
			Processed: true,
		}, nil
	}

	chkIds := make([]string, 0, len(request.Refs))
	for _, ref := range request.Refs {
		id := c.schema.ExternalKey(*ref)
		chkIds = append(chkIds, id)
		level.Debug(util_log.Logger).Log("msg", "received chunk", "bucket", bloomBucket(ref.Through.Time()), "chunk", id)
	}
	err := c.wal.Write(chkIds)
	if err != nil {
		return nil, err
	}
	return &ComputeResponse{Processed: true}, nil
}

func (c *BBFComputer) starting(ctx context.Context) error {
	var err error

	if c.subservices, err = services.NewManager(c.pool); err != nil {
		return errors.Wrap(err, "unable to create computer subservices manager")
	}

	c.subservicesWatcher = services.NewFailureWatcher()
	c.subservicesWatcher.WatchManager(c.subservices)

	if err = services.StartManagerAndAwaitHealthy(ctx, c.subservices); err != nil {
		return errors.Wrap(err, "unable to start computer subservices")
	}

	return nil
}

func (c *BBFComputer) running(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (c *BBFComputer) stopping(_ error) error {
	err := c.wal.Close()
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "error closing WAL", "err", err)
	}

	c.cancel()
	if c.subservices != nil {
		_ = services.StopManagerAndAwaitStopped(context.Background(), c.subservices)
	}
	return err
}

type WAL struct {
	ctx    context.Context
	cancel context.CancelFunc

	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex

	filePath    string
	walFileName string
	maxSize     int64

	recentCreateTime time.Time
}

func generateWalFilePath(filePath string) string {
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("%s.%d", filePath, timestamp)
}

func NewWAL(ctx context.Context, filePath string, maxSize int64) (*WAL, error) {

	walFileName := generateWalFilePath(filePath)

	file, err := os.OpenFile(walFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}

	err = saveOffset(walFileName+".offset", 0)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	wal := &WAL{
		ctx:              ctx,
		cancel:           cancel,
		file:             file,
		writer:           bufio.NewWriter(file),
		filePath:         filePath,
		walFileName:      walFileName,
		maxSize:          maxSize,
		recentCreateTime: time.Now(),
	}
	return wal, nil
}

func loadOffset(offsetFile string) int64 {
	file, err := os.Open(offsetFile)
	if err != nil {
		if !os.IsNotExist(err) {
			level.Error(util_log.Logger).Log("msg", "error opening offset file", "err", err)
		}
		return 0
	}
	defer file.Close()

	var offset int64
	_, err = fmt.Fscanf(file, "%d", &offset)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "error reading offset file", "err", err)
		return 0
	}
	return offset
}

func saveOffset(offsetFile string, offset int64) error {
	file, err := os.OpenFile(offsetFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "error opening offset file", "err", err)
		return err
	}
	defer file.Close()

	_, err = fmt.Fprintf(file, "%d", offset)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "error writing offset file", "err", err)
		return err
	}
	return nil
}

func (wal *WAL) Write(chunkIDs []string) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Check file size and rotate if necessary
	fileInfo, err := wal.file.Stat()
	if err != nil {
		return err
	}
	if fileInfo.Size() >= wal.maxSize {
		_ = level.Debug(util_log.Logger).Log("msg", "rotating WAL", "currentSize", fileInfo.Size(), "maxSize", wal.maxSize)
		if err := wal.rotateWAL(); err != nil {
			return err
		}
	}

	for _, chunkID := range chunkIDs {
		_, err = wal.writer.WriteString(chunkID + "\n")
	}
	if err != nil {
		return err
	}
	return wal.writer.Flush()
}

func (wal *WAL) rotateWAL() error {

	// Close current file
	if err := wal.writer.Flush(); err != nil {
		return err
	}
	if err := wal.file.Close(); err != nil {
		return err
	}

	// Rename current file
	newWalName := generateWalFilePath(wal.filePath)
	newOffsetName := fmt.Sprintf("%s.offset", newWalName)

	// Create new file and offset file
	file, err := os.OpenFile(newWalName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrap(err, "error creating new WAL file when rotating")
	}
	err = saveOffset(newOffsetName, 0)
	if err != nil {
		return errors.Wrap(err, "error creating new offset file when rotating")
	}

	wal.file = file
	wal.writer = bufio.NewWriter(file)
	wal.recentCreateTime = time.Now()
	return nil
}

func (wal *WAL) Read(batch, parallelism int, compute func(ctx context.Context, chunks []string)) error {
	for {
		select {
		case <-wal.ctx.Done():
			return nil
		default:
			files, err := wal.listFiles()
			if err != nil {
				return err
			}

			files, err = wal.deleteOldFile(files)
			if err != nil {
				return err
			}

			for _, file := range files {
				if strings.Contains(file, ".offset") {
					continue
				}
				err = wal.readOne(file, batch, parallelism, compute)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (wal *WAL) readOne(file string, batch, parallelism int, compute func(ctx context.Context, chunks []string)) error {
	offsetFile := file + ".offset"
	offset := loadOffset(offsetFile)

	f, err := os.OpenFile(file, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			_ = level.Error(util_log.Logger).Log("msg", "error closing file", "file", f.Name(), "err", err)
		}
	}(f)

	// Move scanner to the last processed offset
	_, err = f.Seek(offset, 0)
	if err != nil {
		return err
	}

	var (
		scanner     = bufio.NewScanner(f)
		chunkIDs    = make([]string, 0, parallelism)
		count       = 0
		totalLength = int64(0)
	)
	for scanner.Scan() {
		select {
		case <-wal.ctx.Done():
			return nil
		default:
			chunkID := scanner.Text()
			chk, err := parseChunkKey(chunkID)
			if err != nil {
				level.Error(util_log.Logger).Log("msg", "error parsing chunk key", "chunkID", chunkID, "err", err)
				return err
			}
			curr := bloomBucket(chk.Through.Time())
			level.Debug(util_log.Logger).Log("msg", "reading chunk", "bucket", curr)

			if count >= batch {
				grps := groupChunkIDs(chunkIDs)
				for _, group := range grps {
					// 调用 compute 函数处理 group
					if len(group) > parallelism {
						// 如果 group 的长度大于 parallelism，分批处理
						for i := 0; i < len(group); i += parallelism {
							end := i + parallelism
							if end > len(group) {
								end = len(group)
							}
							// TODO anan: 没有对 compute 错误进行返回和处理，有问题
							compute(wal.ctx, group[i:end])
						}
					} else {
						compute(wal.ctx, group)
					}
				}
				// 清空 chunkIDs 切片和重置计数器
				chunkIDs = chunkIDs[:0]
				count = 0

				// 更新 offset
				offset += totalLength
				err = saveOffset(offsetFile, offset)
				if err != nil {
					return errors.Wrap(err, "error saving offset")
				}
				totalLength = 0

			}
			chunkIDs = append(chunkIDs, chunkID)
			count++
			totalLength += int64(len(chunkID) + 1) // +1 for newline character
		}
	}

	// 处理剩余的 chunkIDs
	if count > 0 {
		grps := groupChunkIDs(chunkIDs)
		for _, group := range grps {
			// 调用 compute 函数处理 group
			if len(group) > parallelism {
				// 如果 group 的长度大于 parallelism，分批处理
				for i := 0; i < len(group); i += parallelism {
					end := i + parallelism
					if end > len(group) {
						end = len(group)
					}
					compute(wal.ctx, group[i:end])
				}
			} else {
				compute(wal.ctx, group)
			}
		}

		// 更新 offset
		offset += totalLength
		err = saveOffset(offsetFile, offset)
		if err != nil {
			return errors.Wrap(err, "error saving offset")
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	} else {
		time.Sleep(1 * time.Second)
	}
	return nil
}

func groupChunkIDs(chunkIDs []string) [][]string {
	sort.Slice(chunkIDs, func(i, j int) bool {
		chk1, _ := parseChunkKey(chunkIDs[i])
		chk2, _ := parseChunkKey(chunkIDs[j])
		return chk1.Through.Time().Before(chk2.Through.Time())
	})

	var (
		chunkIDBuckets [][]string
		prevBucket     string
	)
	for _, chunkID := range chunkIDs {
		chk, _ := parseChunkKey(chunkID)
		bucket := bloomBucket(chk.Through.Time())
		if separateDifferentBucket(bucket, prevBucket) {
			chunkIDBuckets = append(chunkIDBuckets, []string{chunkID})
			prevBucket = bucket
		} else {
			if len(prevBucket) == 0 {
				chunkIDBuckets = append(chunkIDBuckets, []string{})
				prevBucket = bucket
			}
			chunkIDBuckets[len(chunkIDBuckets)-1] = append(chunkIDBuckets[len(chunkIDBuckets)-1], chunkID)
		}
	}
	return chunkIDBuckets
}

func separateDifferentBucket(currBucket, prevBucket string) bool {
	// 还没有 prevBucket，不需要切分
	if len(prevBucket) == 0 {
		return false
	}
	return currBucket != prevBucket
}

func (wal *WAL) listFiles() ([]string, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// List all files with the wal.filePath prefix
	files, err := filepath.Glob(wal.filePath + "*")
	if err != nil {
		return nil, err
	}

	// Sort files by modification time
	sort.Slice(files, func(i, j int) bool {
		infoI, _ := os.Stat(files[i])
		infoJ, _ := os.Stat(files[j])
		return infoI.ModTime().Before(infoJ.ModTime())
	})

	return files, err
}

func (wal *WAL) deleteOldFile(files []string) ([]string, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// 只有最新文件时直接返回，不删除
	if len(files) <= 2 {
		return files, nil
	}

	usedFiles := make([]string, 0, len(files))
	i := 0
	for ; i < len(files)-2; i++ {
		if strings.Contains(files[i], ".offset") {
			continue
		}
		offsetFile := files[i] + ".offset"
		offset := loadOffset(offsetFile)

		fileInfo, err := os.Stat(files[i])
		if err != nil {
			return nil, err
		}

		if fileInfo.ModTime().Before(wal.recentCreateTime) && offset == fileInfo.Size() {
			if err = os.Remove(files[i]); err != nil {
				return nil, err
			}
			_ = level.Debug(util_log.Logger).Log("msg", "delete old file success", "file", files[i])
			if err = os.Remove(offsetFile); err != nil {
				return nil, err
			}
			_ = level.Debug(util_log.Logger).Log("msg", "delete old offset file success", "file", offsetFile)
		} else {
			usedFiles = append(usedFiles, offsetFile, files[i])
		}
	}
	clear(files)
	return usedFiles, nil
}

func (wal *WAL) monitorUnreadOffset(UnreadOffsetGauge prometheus.Gauge) {
	for {
		select {
		case <-wal.ctx.Done():
			return
		default:
			files, err := wal.listFiles()
			if err != nil {
				_ = level.Error(util_log.Logger).Log("msg", "error sorting files", "err", err)
				continue
			}

			if len(files) < 2 {
				time.Sleep(defaultMonitorTimes)
				continue
			}

			unread := int64(0)
			for i := len(files) - 1; i >= 0; i-- {
				if strings.Contains(files[i], ".offset") {
					continue
				}
				offsetFile := files[i] + ".offset"
				offset := loadOffset(offsetFile)

				fileInfo, err := os.Stat(files[i])
				if err != nil {
					_ = level.Error(util_log.Logger).Log("msg", "error getting file info", "file", files[i], "err", err)
					continue
				}

				unread -= offset
				unread += fileInfo.Size()
			}

			UnreadOffsetGauge.Set(float64(unread))
			time.Sleep(defaultMonitorTimes)
		}
	}
}

func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	wal.cancel()
	if err := wal.writer.Flush(); err != nil {
		return err
	}
	if err := wal.file.Close(); err != nil {
		return err
	}
	return nil
}
