package bbf

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/loki/v3/pkg/storage/config"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	lokiring "github.com/grafana/loki/v3/pkg/util/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"go.uber.org/atomic"
)

const (
	ReplicationFactor = 1
	NumTokens         = 1
)

var bbfNotFoundErr = errors.New("bbf not found")

type BBFManager struct {
	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
	mu sync.RWMutex
	// shard lock
	createLocker *locker[string]
	// cache the bbf meta data, if not in cache, get from disk
	bbfs map[string]*BBF

	rm          *lokiring.RingManager
	stickyRing  *StickyRing
	bbfStore    Store
	schema      config.SchemaConfig
	cfg         Config
	window      *Window
	metaManager *MetaManager

	FlushElapsedGauge       prometheus.Gauge
	FlushFiltersGauge       prometheus.Gauge
	FilterAppendedHistogram prometheus.Histogram
	ShardFiltersStatsGauge  *prometheus.GaugeVec
}

func NewBBFManger(
	cfg Config,
	periods []config.PeriodConfig,
	bbfStore Store,
	rm *lokiring.RingManager,
	reg prometheus.Registerer,
	logger log.Logger,
) (*BBFManager, error) {
	sr, err := NewStickyRing(100, rm.Ring.KVClient, rm.Ring)
	if err != nil {
		return nil, fmt.Errorf("create sticky ring failed: %w", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("get hostname failed: %w", err)
	}
	kvClient, err := kv.NewClient(
		cfg.RingConfig.KVStore,
		codec.String{},
		reg,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("create kv client failed: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &BBFManager{
		ctx:          ctx,
		cancel:       cancel,
		cfg:          cfg,
		bbfs:         make(map[string]*BBF),
		createLocker: newLocker[string](),
		stickyRing:   sr,
		rm:           rm,
		bbfStore:     bbfStore,
		window:       NewWindow(ctx, hostname, cfg.WindowSize, kvClient, reg),
		metaManager:  NewMetaManager(ctx, bbfStore),
		schema: config.SchemaConfig{
			Configs: periods,
		},
		FlushElapsedGauge: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "loki_bbf_flush_elapsed_milliseconds",
			Help: "Time spend doing flush to bbf.",
		}),
		FlushFiltersGauge: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "loki_bbf_flush_filters",
			Help: "The number of filters flushed to bbf.",
		}),
		FilterAppendedHistogram: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "loki_bbf_filter_appended",
			Help:    "The number of values appended to filter, can be used to observe filter fill rates.",
			Buckets: prometheus.LinearBuckets(100_000, 200_000, 7),
		}),
		ShardFiltersStatsGauge: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "loki_bbf_shard_filters_stats",
			Help: "Shard filters stats.",
		}, []string{"category"}),
	}

	loops := []func(){
		m.flushLoop,
		m.gcLoop,
		m.statsLoop,
	}
	m.wg.Add(len(loops))
	for _, l := range loops {
		go func(f func()) {
			f()
			m.wg.Done()
		}(l)
	}

	return m, nil
}

func (m *BBFManager) Add(ctx context.Context, request *AddRequest) (*AddResponse, error) {
	if len(request.Records) == 0 {
		return nil, fmt.Errorf("request is empty")
	}
	for _, record := range request.Records {
		if len(record.Values) == 0 {
			continue
		}
		filter := buildChunkBloomFilterKey(record.Ref)
		if len(filter) == 0 {
			return nil, fmt.Errorf("filter name is empty")
		}
		if len(record.Values) == 0 {
			return nil, fmt.Errorf("values is empty")
		}

		bbf, err := m.GetOrCreateBBF(filter, bloomBucketTime(record.Ref.Through))
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "Add get bbf failed", "err", err)
			return nil, err
		}

		chunkId := m.schema.ExternalKey(*record.Ref)
		err = bbf.AddTo(int(record.Shard), record.Values, chunkId)
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "add to bbf failed", "err", err)
			return nil, err
		}
	}
	return &AddResponse{Added: true}, nil
}

func (m *BBFManager) GetBBFShard(name string, shard int, bucket model.Time) (*BBFShard, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	bbf, ok := m.bbfs[name]
	if !ok {
		s := newBBFShard(m, name, shard, bucket, nil)
		return s, nil
	}

	s, sok := bbf.Shards[shard]
	if !sok {
		s = newBBFShard(m, name, shard, bucket, nil)
		bbf.Shards[shard] = s
		return s, nil
	}

	return s, nil
}

func (m *BBFManager) BBFExists(ctx context.Context, request *BBFExistsRequest) (*BBFExistsResponse, error) {
	if len(request.Filter) == 0 {
		return nil, fmt.Errorf("filter name is empty")
	}
	if len(request.Value) == 0 {
		return nil, fmt.Errorf("values is empty")
	}
	if len(request.Refs) == 0 {
		return nil, fmt.Errorf("refs is empty")
	}

	start := time.Now()
	bbfShard, err := m.GetBBFShard(request.Filter, int(request.Shard), request.Refs[0].Through)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "GetBBFShard failed", "err", err)
		return nil, err
	}
	if time.Since(start) > 500*time.Millisecond {
		level.Warn(util_log.Logger).Log("msg", "slow GetBBFShard duration", "duration", time.Since(start))
	}

	if bbfShard == nil {
		level.Error(util_log.Logger).Log("msg", "GetBBFShard failed, bbf shard is nil, will all return contains", "filter", request.Filter, "shard", request.Shard)
		// 返回默认全为 true
		return buildAllPassResponse(len(request.Refs)), nil
	}

	filterStart := time.Now()
	// 如果filter 级别不存在，则不用判断每个 chunk
	res, exists, err := bbfShard.Contains(ctx, []string{request.Value})
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "BBFExists failed", "err", err)
		return nil, err
	}
	if time.Since(filterStart) > 500*time.Millisecond {
		level.Warn(util_log.Logger).Log("msg", "slow BBFExists 1 duration", "duration", time.Since(filterStart))
	}
	if !res[0] {
		level.Debug(util_log.Logger).Log("msg", "BBFExists result", "token", request.Value, "len(refs)", len(request.Refs), "exists", exists, "duration", time.Since(start))
		return buildAllOutResponse(len(request.Refs)), nil
	}

	tokens := make([]string, 0, len(request.Refs))
	for _, ref := range request.Refs {
		chunkId := m.schema.ExternalKey(*ref)
		tokens = append(tokens, request.Value+chunkId)
	}

	filterStart = time.Now()
	res, exists, err = bbfShard.Contains(ctx, tokens)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "BBFExists failed", "err", err)
		return nil, err
	}
	if time.Since(filterStart) > 500*time.Millisecond {
		level.Warn(util_log.Logger).Log("msg", "slow BBFExists 2 duration", "duration", time.Since(filterStart))
	}

	level.Debug(util_log.Logger).Log("msg", "BBFExists result", "token", request.Value, "len(refs)", len(request.Refs), "exists", exists, "duration", time.Since(start))
	return &BBFExistsResponse{Exists: res}, nil
}

func buildAllPassResponse(refs int) *BBFExistsResponse {
	res := make([]bool, 0, refs)
	for i := 0; i < refs; i++ {
		res = append(res, true)
	}
	return &BBFExistsResponse{Exists: res}
}

func buildAllOutResponse(refs int) *BBFExistsResponse {
	res := make([]bool, 0, refs)
	for i := 0; i < refs; i++ {
		res = append(res, false)
	}
	return &BBFExistsResponse{Exists: res}
}

func (m *BBFManager) mustEmbedUnimplementedBBFServer() {}

func (m *BBFManager) getBBFDir(prefix string) string {
	return filepath.Join(m.cfg.Path, prefix)
}

func (m *BBFManager) getAllBBFs() []*BBF {
	m.mu.Lock()
	defer m.mu.Unlock()

	bbfs := make([]*BBF, 0, len(m.bbfs))
	for _, bbf := range m.bbfs {
		bbfs = append(bbfs, bbf)
	}
	return bbfs
}

func (m *BBFManager) GetBBF(name string) (*BBF, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	bbf, ok := m.bbfs[name]
	if !ok {
		return nil, bbfNotFoundErr
	}
	return bbf, nil
}

// GetOrCreateBBF get or create a bbf if the bbf is not exist
func (m *BBFManager) GetOrCreateBBF(name string, bucket model.Time) (*BBF, error) {
	m.createLocker.Lock(name)
	defer m.createLocker.Unlock(name)

	m.mu.RLock()
	bbf, ok := m.bbfs[name]
	m.mu.RUnlock()
	if ok {
		return bbf, nil
	}

	// 检查 window 是否允许创建新的 bbf
	allow, err := m.window.Expand(bloomBucket(bucket.Time()))
	if err != nil {
		return nil, err
	}
	if !allow {
		return nil, fmt.Errorf("bucket %s is not allowed due to window size limit, window size now is %d", bloomBucket(bucket.Time()), m.window.Size())
	}

	bbf, err = NewBBF(name, bucket, m)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.bbfs[name] = bbf
	m.mu.Unlock()
	return bbf, nil
}

func (m *BBFManager) deleteBBF(dirName string) error {
	dir := m.getBBFDir(dirName)

	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("remove dir %s failed: %w", dir, err)
	}

	return nil
}

func (m *BBFManager) flushLoop() {
	ticker := time.NewTicker(m.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			//_ = level.Info(util_log.Logger).Log("msg", "flushing bbf...")
			m.flush(false)
		case <-m.ctx.Done():
			_ = level.Info(util_log.Logger).Log("msg", "flushing bbf before shutdown...")
			m.flush(true)
			return
		}
	}
}

// flush will flush all the bbf to disk, if immediate is true, flush all the bbf to disk immediately
// otherwise, flush the bbf if the bbf should flush
func (m *BBFManager) flush(immediate bool) {
	var (
		start = time.Now()
		count atomic.Int32
	)
	bbfs := m.getAllBBFs()
	for _, bbf := range bbfs {

		if !immediate && !bbf.ShouldFlush() {
			continue
		}
		shards := bbf.getNeedFlushShards()
		level.Debug(util_log.Logger).Log("msg", "flushing bbf", "bbf", bbf.Name, "shards", len(shards))
		// 写死 20 并发
		err := concurrency.ForEachJob(context.Background(), len(shards), 20, func(ctx context.Context, i int) error {
			count.Add(int32(shards[i].mustFlush(ctx)))
			return nil
		})
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "flush bbf failed", "err", err)
		}
		err = m.window.Shrink(bloomBucket(bbf.Bucket.Time()))
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "shrink window failed", "err", err)
		}
		level.Debug(util_log.Logger).Log("msg", "shrink window", "bbf", bbf.Name, "bucket", bbf.Bucket, "window", m.window.String())

		m.mu.Lock()
		delete(m.bbfs, bbf.Name)
		m.mu.Unlock()
	}

	level.Info(util_log.Logger).Log("msg", "flushed bbf", "bbfs", len(bbfs), "elapsed", time.Since(start), "filters count", count.Load())
	m.FlushElapsedGauge.Set(float64(time.Since(start).Milliseconds()))
	m.FlushFiltersGauge.Set(float64(count.Load()))
}

func (m *BBFManager) gcLoop() {
	ticker := time.NewTicker(m.cfg.GcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.gc()
		case <-m.ctx.Done():
			return
		}
	}
}

// gc will delete the bbf file out side of the retention
func (m *BBFManager) gc() {
	entries, err := os.ReadDir(m.cfg.Path)
	if err != nil {
		_ = level.Error(util_log.Logger).Log("msg", "read dir failed", "err", err)
		return
	}

	for _, file := range entries {
		if !file.IsDir() {
			continue
		}

		createTime, err := time.Parse("200601021504", file.Name())
		if err != nil {
			_ = level.Error(util_log.Logger).Log("msg", "parse create time failed", "file name", file.Name(), "err", err)
			continue
		}

		if time.Since(createTime) <= m.cfg.Retention {
			continue
		}

		if err = m.deleteBBF(file.Name()); err != nil {
			level.Error(util_log.Logger).Log("msg", "delete bbf failed", "err", err)
		} else {
			level.Debug(util_log.Logger).Log("msg", "delete bbf success", "name", file.Name())
		}
	}
}

func (m *BBFManager) statsLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.stats()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *BBFManager) stats() {
	var (
		bbfCount = 0
		shards   = 0
		filters  = 0
		bbfs     = m.getAllBBFs()
	)
	for _, bbf := range bbfs {
		notEmptyShards := 0
		for _, shard := range bbf.Shards {
			if len(shard.filters) > 0 {
				filters += len(shard.filters)
				notEmptyShards++
			}
			for _, filter := range shard.filters {
				m.FilterAppendedHistogram.Observe(float64(filter.appended))
			}
		}
		if notEmptyShards > 0 {
			shards += notEmptyShards
			bbfCount++
		}
	}
	m.ShardFiltersStatsGauge.WithLabelValues("bbfs").Set(float64(bbfCount))
	m.ShardFiltersStatsGauge.WithLabelValues("shards").Set(float64(shards))
	m.ShardFiltersStatsGauge.WithLabelValues("filters").Set(float64(filters))
}

func (m *BBFManager) Close() {
	m.cancel()
	m.wg.Wait()
	m.flush(true)
}

func (m *BBFManager) FlushHandler(writer http.ResponseWriter, _ *http.Request) {
	m.flush(true)
	writer.WriteHeader(http.StatusOK)
}

func (m *BBFManager) DebugHandler(writer http.ResponseWriter, _ *http.Request) {
	bbfs := m.getAllBBFs()

	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, bbf := range bbfs {
		_, _ = fmt.Fprintf(writer, "BBFName: %s, should flush: %t\n", bbf.Name, bbf.ShouldFlush())
		for shardID, shard := range bbf.Shards {
			_, _ = fmt.Fprintf(writer, "BBF: %s, Shard: %d, filters: %d\n", bbf.Name, shardID, len(shard.filters))
			for _, filter := range shard.filters {
				_, _ = fmt.Fprintf(writer, "Filter idx=%d, Appended=%d\n", filter.idx, filter.appended)
			}
		}
	}
	_, _ = fmt.Fprintf(writer, "Window: %s\n", m.window.String())
	writer.WriteHeader(http.StatusOK)
}

func (m *BBFManager) MetaHandler(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	if len(prefix) == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}
	fs, err := m.metaManager.Filters(prefix)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "failed to get filters", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, _ = w.Write([]byte(fmt.Sprintf("filters: %v", fs)))
	w.WriteHeader(http.StatusOK)
}

// mockBBFManager，用于测试
type mockBBFManager struct {
}

func (m *mockBBFManager) Add(ctx context.Context, request *AddRequest) (*AddResponse, error) {
	return &AddResponse{Added: true}, nil
}

func (m *mockBBFManager) BBFExists(ctx context.Context, request *BBFExistsRequest) (*BBFExistsResponse, error) {
	return nil, nil
}

func (m *mockBBFManager) mustEmbedUnimplementedBBFServer() {}

func (m *mockBBFManager) Close() {}

func (m *mockBBFManager) FlushHandler(writer http.ResponseWriter, request *http.Request) {}
