package bbf

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/loki/v3/pkg/chunkenc"
	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	logql_log "github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logqlmodel/stats"
	"github.com/grafana/loki/v3/pkg/storage"
	"github.com/grafana/loki/v3/pkg/storage/chunk"
	"github.com/grafana/loki/v3/pkg/storage/chunk/cache"
	"github.com/grafana/loki/v3/pkg/storage/chunk/client"
	"github.com/grafana/loki/v3/pkg/storage/config"
	"github.com/grafana/loki/v3/pkg/storage/stores"
	"github.com/grafana/loki/v3/pkg/util"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/grafana/loki/v3/pkg/util/spanlogger"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
)

var (
	EmptyFilterObjError   = errors.New("empty filter object")
	ObjNotExistError      = errors.New("object not exist")
	downloadQueueCapacity = 10000
)

type Store interface {
	LoadFilter(ctx context.Context, name string, ts model.Time, pool bool) (*bloomFilter, error)
	UploadFilter(ctx context.Context, key string, dst []byte, ts model.Time) error
	ListObjectWithPrefix(ctx context.Context, prefix string, ts model.Time) ([]string, error)
	LoadChunk(ctx context.Context, chk chunk.Chunk) (iter.EntryIterator, error)
	GetObject(ctx context.Context, objKey string, ts model.Time) ([]byte, error)
	PutObject(ctx context.Context, objKey string, data []byte, ts model.Time) error
	LoadFilterParallelForRead(ctx context.Context, objs []string, bucket model.Time) ([]*bloomFilter, error)
	LoadFilterFromDisk(filterName string) (*bloomFilter, error)
}

type bbfStoreEntry struct {
	objectClient client.ObjectClient
	start        model.Time
	cfg          config.PeriodConfig

	q        *util.DownloadQueue[string, *bloomFilter]
	bbfCache cache.TypedCache[string, *bloomFilter]
	metrics  *Metrics
}

type BBFStore struct {
	fetcherProvider stores.ChunkFetcherProvider
	periods         []config.PeriodConfig
	stores          []*bbfStoreEntry
	metrics         *Metrics
}

func (b *BBFStore) Stop() {
	for _, store := range b.stores {
		store.objectClient.Stop()
	}
}

func NewBBFStore(store storage.Store,
	cfg Config,
	storageConfig storage.Config,
	clientMetrics storage.ClientMetrics,
	reg prometheus.Registerer,
	logger log.Logger,
	cacheType stats.CacheType,
) *BBFStore {
	cacheCfg := cfg.CacheCfg.EmbeddedCache
	cacheCfg.Enabled = true
	cacheCfg.PurgeInterval = 10 * time.Second
	bbfCache := cache.NewTypedEmbeddedCache[string, *bloomFilter](
		"bbf-filter-lru",
		cacheCfg,
		reg,
		logger,
		cacheType,
		func(entry *cache.Entry[string, *bloomFilter]) uint64 { return uint64(cfg.ShardCapacity * 2) },
		nil,
	)

	s := &BBFStore{
		fetcherProvider: store,
		periods:         store.GetSchemaConfigs(),
		metrics:         NewMetrics(reg),
	}

	for _, periodicConfig := range s.periods {
		objectClient, err := storage.NewObjectClient(periodicConfig.ObjectType, "bbf", storageConfig, clientMetrics)
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "failed to create object client", "error", err)
			return nil
		}
		objectClient = newCachedListOpObjectClient(objectClient, 1*time.Minute, 10*time.Second)

		e := &bbfStoreEntry{
			objectClient: objectClient,
			start:        periodicConfig.From.Time,
			cfg:          periodicConfig,
			bbfCache:     bbfCache,
			metrics:      s.metrics,
		}
		queue, err := util.NewDownloadQueue[string, *bloomFilter](downloadQueueCapacity, cfg.FilterDownloadParallelism, e.processDownload, util_log.Logger)
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "failed to create download queue", "error", err)
			return nil
		}
		e.q = queue
		s.stores = append(s.stores, e)
	}
	return s
}

func (s *bbfStoreEntry) processDownload(ctx context.Context, req util.DownloadRequest[string, *bloomFilter]) {
	// 双重检测
	// 1. 从缓存中加载
	_, filters, _, err := s.bbfCache.Fetch(ctx, []string{req.Item})
	if err != nil {
		req.Errors <- err
		return
	}
	if len(filters) > 0 {
		s.metrics.ReadFromCacheFiltersCounter.Inc()
		req.Results <- util.DownloadResponse[*bloomFilter]{
			Item: filters[0],
			Key:  req.Key,
			Idx:  req.Idx,
		}
		return
	}

	// 2. 从 S3 中加载
	start := time.Now()
	data, l, err := s.objectClient.GetObject(ctx, req.Item) // 查询不使用对象池
	if err != nil {
		req.Errors <- err
		return
	}
	if l == 0 {
		req.Errors <- EmptyFilterObjError
		return
	}

	duration := time.Since(start)
	if duration > 500*time.Millisecond {
		level.Warn(util_log.Logger).Log("msg", "load filter from object storage", "obj", req.Item, "duration", duration)
	}
	s.metrics.FetchFilterObjElapsedHistogram.WithLabelValues("read").Observe(float64(time.Since(start).Milliseconds()))
	s.metrics.LoadFromS3FiltersCounter.WithLabelValues("read").Inc()

	// 3. 写入缓存
	filter, err := decodeFilter(data, l, false)
	if err := s.bbfCache.Store(ctx, []string{req.Item}, []*bloomFilter{filter}); err != nil {
		req.Errors <- err
		return
	}

	req.Results <- util.DownloadResponse[*bloomFilter]{
		Item: filter,
		Key:  req.Key,
		Idx:  req.Idx,
	}
}

// loadFilterFromS3WithCache 从缓存或 S3 加载 filter，加载过程中加锁防止多个并发请求重复 I/O
func (b *BBFStore) LoadFilterParallelForRead(ctx context.Context, objs []string, bucket model.Time) ([]*bloomFilter, error) {
	store := b.getStore(bucket)
	// 1. 从缓存中加载
	_, filters, missing, err := store.bbfCache.Fetch(ctx, objs)
	if err != nil {
		return nil, err
	}
	if len(missing) == 0 {
		b.metrics.ReadFromCacheFiltersCounter.Inc()
		return filters, nil
	}

	var (
		response = make(chan util.DownloadResponse[*bloomFilter], len(missing))
		errChan  = make(chan error, len(missing))
		start    = time.Now()
	)

	for i, obj := range missing {
		store.q.Enqueue(util.DownloadRequest[string, *bloomFilter]{
			Ctx:     ctx,
			Item:    obj,
			Key:     obj,
			Idx:     i,
			Async:   false,
			Errors:  errChan,
			Results: response,
		})
	}
	b.metrics.DownloadQueueEnqueueTime.Observe(time.Since(start).Seconds())

	for i := 0; i < len(missing); i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err = <-errChan:
			return nil, err
		case res := <-response:
			filters = append(filters, res.Item)
		}
	}
	return filters, nil
}

// LoadFilter 查询时不应使用对象池，因为很难找到合适的时机释放对象，除非给 TypedCache 加个缓存驱逐的回调
// 写入时应使用对象池，写入的对象在 Flush 时会被释放回对象池
func (b *BBFStore) LoadFilter(ctx context.Context, name string, ts model.Time, pool bool) (*bloomFilter, error) {

	store := b.getStore(ts)
	data, l, err := store.objectClient.GetObject(ctx, name)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, ObjNotExistError
	}

	// TODO anan: data 怎么会为空
	defer func(data io.ReadCloser) {
		if data == nil {
			return
		}
		err := data.Close()
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "failed to close data", "error", err)
		}
	}(data)

	return decodeFilter(data, l, pool)
}

func decodeFilter(data io.ReadCloser, l int64, usePool bool) (*bloomFilter, error) {
	dst := byteSlicePool.Get().([]byte)[:l]
	defer byteSlicePool.Put(dst[:0]) // Reset the slice before putting it back

	n, err := io.ReadFull(data, dst)
	if err != nil && err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	dst = dst[:n]
	if len(dst) == 0 {
		return nil, EmptyFilterObjError
	}

	var filter *bloomFilter
	if usePool {
		filter = getBloomFilter()
	} else {
		// 查询时不应从对象池获取，因为很难找到合适的时机释放对象，除非给 TypedCache 加个缓存驱逐的回调
		filter = &bloomFilter{}
	}
	err = filter.unmarshal(dst)
	if err != nil {
		return nil, err
	}
	return filter, err
}

// LoadFilterFromDisk 从磁盘加载 filter
func (b *BBFStore) LoadFilterFromDisk(filterName string) (*bloomFilter, error) {
	start := time.Now()
	defer b.metrics.DiskReadElapsedHistogram.WithLabelValues("write").Observe(float64(time.Since(start).Milliseconds()))

	file, err := os.Open(filterName)
	if err != nil {
		return nil, fmt.Errorf("open file %s failed: %w", filterName, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file %s failed: %w", filterName, err)
	}

	return decodeFilter(file, fileInfo.Size(), false)
}

func (b *BBFStore) UploadFilter(ctx context.Context, key string, dst []byte, ts model.Time) error {
	store := b.getStore(ts)
	if len(dst) == 0 {
		level.Error(util_log.Logger).Log("msg", "Uploading empty filter data", "key", key)
	}
	return store.objectClient.PutObject(ctx, key, bytes.NewReader(dst))
}

func (b *BBFStore) ListObjectWithPrefix(ctx context.Context, prefix string, ts model.Time) ([]string, error) {
	var (
		objects []string
		lastErr error
		list    []client.StorageObject
		store   = b.getStore(ts)
	)
	bkcfg := backoff.Config{
		MinBackoff: 1 * time.Second,
		MaxBackoff: 5 * time.Second,
		MaxRetries: 3,
	}
	retries := backoff.New(ctx, bkcfg)
	for retries.Ongoing() {
		list, _, lastErr = store.objectClient.List(ctx, prefix, "")
		if lastErr == nil {
			break
		}
		retries.Wait()
	}

	if lastErr != nil {
		return nil, lastErr
	}
	for _, object := range list {
		objects = append(objects, object.Key)
	}
	return objects, nil
}

func (b *BBFStore) GetObject(ctx context.Context, objKey string, ts model.Time) ([]byte, error) {
	store := b.getStore(ts)
	object, _, err := store.objectClient.GetObject(ctx, objKey)
	if err != nil {
		return nil, err
	}

	defer func(object io.ReadCloser) {
		if object == nil {
			return
		}
		err := object.Close()
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "failed to close object", "error", err)
		}
	}(object)

	var buf bytes.Buffer
	_, err = io.Copy(&buf, object)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (b *BBFStore) PutObject(ctx context.Context, objKey string, data []byte, ts model.Time) error {
	store := b.getStore(ts)
	return store.objectClient.PutObject(ctx, objKey, bytes.NewReader(data))
}

func (b *BBFStore) LoadChunk(ctx context.Context, chk chunk.Chunk) (iter.EntryIterator, error) {
	f := b.fetcherProvider.GetChunkFetcher(chk.From)
	cs, err := f.FetchChunks(ctx, []chunk.Chunk{chk})
	if err != nil {
		return nil, err
	}
	if len(cs) != 1 {
		return nil, fmt.Errorf("expected 1 chunk, got %d", len(cs))
	}
	// 从 newBatchedChunkLoader(pkg/bbf/batch.go:115) 拷贝
	c := cs[0].Data.(*chunkenc.Facade).LokiChunk()
	itr, err := c.Iterator(
		ctx,
		time.Unix(0, 0),
		time.Unix(0, math.MaxInt64),
		logproto.FORWARD,
		logql_log.NewNoopPipeline().ForStream(nil),
	)
	return itr, err
}

func (b *BBFStore) getStore(ts model.Time) *bbfStoreEntry {
	// find the schema with the lowest start _after_ tm
	j := sort.Search(len(b.stores), func(j int) bool {
		return b.stores[j].start > ts
	})

	// reduce it by 1 because we want a schema with start <= tm
	j--

	if 0 <= j && j < len(b.stores) {
		return b.stores[j]
	}

	// should in theory never happen
	return nil
}

func (b *BBFStore) storeDo(ts model.Time, f func(s *bbfStoreEntry) error) error {
	if store := b.getStore(ts); store != nil {
		return f(store)
	}
	return fmt.Errorf("no store found for timestamp %s", ts.Time())
}

type mockStore struct{}

func (m *mockStore) LoadFilterParallelForRead(ctx context.Context, objs []string, bucket model.Time) ([]*bloomFilter, error) {
	return nil, nil
}

func (m *mockStore) LoadFilterFromDisk(filterName string) (*bloomFilter, error) {
	return nil, nil
}

func (m *mockStore) GetObject(ctx context.Context, objKey string, ts model.Time) ([]byte, error) {
	return []byte{}, nil
}

func (m *mockStore) PutObject(ctx context.Context, objKey string, data []byte, ts model.Time) error {
	return nil
}

func (m *mockStore) LoadFilter(ctx context.Context, name string, ts model.Time, pool bool) (*bloomFilter, error) {
	// 模拟返回一个空的 bloomFilter 和 nil 错误
	return &bloomFilter{}, nil
}

func (m *mockStore) UploadFilter(ctx context.Context, key string, dst []byte, ts model.Time) error {
	// 模拟成功上传，返回 nil 错误
	return nil
}

func (m *mockStore) ListObjectWithPrefix(ctx context.Context, prefix string, ts model.Time) ([]string, error) {
	// 模拟返回一个空的对象列表和 nil 错误
	return []string{}, nil
}

func (m *mockStore) LoadChunk(ctx context.Context, chk chunk.Chunk) (iter.EntryIterator, error) {
	// 模拟返回一个空的 EntryIterator 和 nil 错误
	return nil, nil
}

type listOpResult struct {
	ts       time.Time
	objects  []client.StorageObject
	prefixes []client.StorageCommonPrefix
}

type listOpCache map[string]listOpResult

type cachedListOpObjectClient struct {
	client.ObjectClient
	cache         listOpCache
	mtx           sync.RWMutex
	ttl, interval time.Duration
	done          chan struct{}
}

func newCachedListOpObjectClient(oc client.ObjectClient, ttl, interval time.Duration) *cachedListOpObjectClient {
	client := &cachedListOpObjectClient{
		ObjectClient: oc,
		cache:        make(listOpCache),
		done:         make(chan struct{}),
		ttl:          ttl,
		interval:     interval,
	}

	go func(c *cachedListOpObjectClient) {
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()

		for {
			select {
			case <-c.done:
				return
			case <-ticker.C:
				c.mtx.Lock()
				for k := range c.cache {
					if time.Since(c.cache[k].ts) > c.ttl {
						delete(c.cache, k)
					}
				}
				c.mtx.Unlock()
			}
		}
	}(client)

	return client
}

func (c *cachedListOpObjectClient) List(ctx context.Context, prefix string, delimiter string) ([]client.StorageObject, []client.StorageCommonPrefix, error) {
	var (
		logger   = spanlogger.FromContext(ctx, util_log.Logger)
		start    = time.Now()
		cacheDur time.Duration
	)
	defer func() {
		logger.LogKV(
			"cache_duration", cacheDur,
			"total_duration", time.Since(start),
		)
	}()

	if delimiter != "" {
		return nil, nil, fmt.Errorf("does not support LIST calls with delimiter: %s", delimiter)
	}
	c.mtx.RLock()
	cached, found := c.cache[prefix]
	c.mtx.RUnlock()
	cacheDur = time.Since(start)
	if found {
		return cached.objects, cached.prefixes, nil
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()

	// double check in case another goroutine has already fetched the list and updated the cache
	cached, found = c.cache[prefix]
	if found {
		return cached.objects, cached.prefixes, nil
	}
	objects, prefixes, err := c.ObjectClient.List(ctx, prefix, delimiter)
	if err != nil {
		return nil, nil, err
	}

	c.cache[prefix] = listOpResult{
		ts:       time.Now(),
		objects:  objects,
		prefixes: prefixes,
	}

	return objects, prefixes, err
}

func (c *cachedListOpObjectClient) Stop() {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	close(c.done)
	c.cache = nil
	c.ObjectClient.Stop()
}
