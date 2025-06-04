package bbf

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/loki/v3/pkg/util"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/prometheus/common/model"
)

type BBFShard struct {
	manager  *BBFManager
	bbfStore Store

	bbfName string
	// shardLocalPath：/${path}/${name}/s${shard}
	// filterName: 	   /${path}/${name}/s${shard}_f{$index}
	shardLocalPath string

	// shardObjectPrefix: BBF/${name}/s${shard}
	// objectKey: 		  BBF/${name}/s${shard}_f{$index}
	shardObjectPrefix string
	bucket            model.Time
	filters           []*bloomFilter
	// cur filter index, -1 means not init
	maxIndex int
	mutex    sync.RWMutex
}

func newBBFShard(m *BBFManager, bbfName string, shard int, bucket model.Time, filters []*bloomFilter) *BBFShard {
	return &BBFShard{
		manager:           m,
		bbfStore:          m.bbfStore,
		bbfName:           bbfName,
		shardLocalPath:    filepath.Join(m.cfg.Path, bbfName, "s"+strconv.Itoa(shard)),
		shardObjectPrefix: fmt.Sprintf("BBF/%s/s%d", bbfName, shard),
		bucket:            bucket,
		filters:           filters,
		maxIndex:          len(filters) - 1,
	}
}

func (s *BBFShard) buildFilterFileName(index int) string {
	return fmt.Sprintf("%s_f%d", s.shardLocalPath, index)
}

func (s *BBFShard) buildFilterOSSKey(index int) string {
	return fmt.Sprintf("%s_f%d", s.shardObjectPrefix, index)
}

func (s *BBFShard) needFlush() bool {
	return len(s.filters) > 0
}

var byteSlicePool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1000000*2+1000)
	},
}

func (s *BBFShard) mustFlush(ctx context.Context) (count int) {
	err := os.MkdirAll(filepath.Dir(s.shardLocalPath), os.ModePerm)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "Error creating parent directories", "dirOf", s.shardLocalPath, "error", err)
		return
	}

	s.mutex.Lock()
	filters := s.filters
	s.mutex.Unlock()

	if len(filters) > 0 {
		for i := len(filters) - 1; i >= 0; i-- {
			filter := filters[i]
			if filter.appended == 0 {
				// 该 filter 为空，不需要 flush
				continue
			}

			fileName := s.buildFilterFileName(filter.idx)

			dst := byteSlicePool.Get().([]byte)[:0] // Get a byte slice from the pool and reset its length
			if cap(dst) < len(filter.bits)*8+16 {
				// Ensure the byte slice is of the required length, this should not happen
				level.Warn(util_log.Logger).Log("msg", "byte slice capacity is not enough", "cap", cap(dst), "required", len(filter.bits)*8+16)
				dst = make([]byte, 0, len(filter.bits)*8+16)
			}

			dst = filter.marshal(dst)
			err = os.WriteFile(fileName, dst, os.ModePerm)
			if err != nil {
				level.Error(util_log.Logger).Log("msg", "flush filter failed", "file", fileName, "error", err)
			}

			// 上传到 S3
			key := s.buildFilterOSSKey(filter.idx)
			err = s.bbfStore.UploadFilter(ctx, key, dst, s.bucket)
			if err != nil {
				level.Error(util_log.Logger).Log("msg", "flush filter to object storage failed", "key", key, "error", err)
			} else {
				s.manager.metaManager.AddFilter(s.shardObjectPrefix, key)
			}
			level.Debug(util_log.Logger).Log("msg", "flush filter", "filter", filter.String(), "file", fileName, "key", key)

			byteSlicePool.Put(dst)
			count++
		}
	}

	s.mutex.Lock()
	for _, filter := range s.filters {
		putBloomFilter(filter)
	}
	s.filters = nil
	s.mutex.Unlock()

	return count
}

var tokensPool = sync.Pool{
	New: func() interface{} {
		return make([]string, 0, 100) // 初始容量可以根据实际情况调整
	},
}

func (s *BBFShard) AddWithSuffix(tokens []string, suffix string) (bool, error) {
	// TODO anan: 锁优化
	s.mutex.Lock()
	defer s.mutex.Unlock()

	filter, err := s.loadNewestFilter()
	if err != nil {
		return false, fmt.Errorf("get newest filter failed: %w", err)
	}

	tokensWithSuffix := tokensPool.Get().([]string)[:0] // 从池中获取切片并重置长度
	defer tokensPool.Put(tokensWithSuffix)              // 使用后将切片放回池中

	var buff = make([]byte, 0, 200) // 临时缓冲区，避免频繁分配内存
	for _, token := range tokens {
		buff = append(buff[:0], util.YoloBuf(token)...) // 重置缓冲区
		buff = append(buff, util.YoloBuf(suffix)...)
		if filter.containsAll([]string{util.YoloString(buff)}) {
			continue
		}
		tokensWithSuffix = append(tokensWithSuffix, string(buff))
	}
	if filter.appended+uint64(len(tokens)) > uint64(s.manager.cfg.ShardCapacity) {
		_ = level.Info(util_log.Logger).Log("msg", "filter is full, need to create a new filter", "tokens", len(tokens), "filter", filter.String())
		// 当前 filter 已满，需要创建新的 filter
		filter = s.createNewFilter()
	}

	filter.Add(tokensWithSuffix)
	return true, nil
}

func (s *BBFShard) Add(tokens []string) (bool, error) {
	// TODO anan: 锁优化
	s.mutex.Lock()
	defer s.mutex.Unlock()

	filter, err := s.loadNewestFilter()
	if err != nil {
		return false, fmt.Errorf("get newest filter failed: %w", err)
	}

	if filter.appended+uint64(len(tokens)) > uint64(s.manager.cfg.ShardCapacity) {
		// 当前 filter 已满，需要创建新的 filter
		filter = s.createNewFilter()
	}

	uniqueTokens := tokensPool.Get().([]string)[:0] // 从池中获取切片并重置长度
	defer tokensPool.Put(uniqueTokens)              // 使用后将切片放回池中

	for _, token := range tokens {
		if filter.containsAll([]string{token}) {
			continue
		}
		uniqueTokens = append(uniqueTokens, token)
	}
	filter.Add(uniqueTokens)
	return true, nil
}

func bypassAllTokens(tokens []string) []bool {
	res := make([]bool, 0, len(tokens))
	for range tokens {
		res = append(res, true)
	}
	return res
}

func (s *BBFShard) Contains(ctx context.Context, tokens []string) ([]bool, int, error) {
	start := time.Now()
	filters, err := s.loadAllFilters(ctx)
	if err != nil {
		return bypassAllTokens(tokens), len(tokens), nil
	}
	if time.Since(start) > 500*time.Millisecond {
		level.Warn(util_log.Logger).Log("msg", "load all filters slow", "duration", time.Since(start))
	}
	if len(filters) == 0 {
		level.Warn(util_log.Logger).Log("msg", "no filters found", "tokens", len(tokens), "shardPrefix", s.shardObjectPrefix)
		return bypassAllTokens(tokens), len(tokens), nil
	}

	// TODO anan: 目前发现有个别 filter appended 元素个数远小于其他分片，这是一种不正常的情况，还需要定位写入的问题，
	// 这里先 hack 一下，如果 filter 中的元素太少，直接跳过，
	if len(filters) == 1 && filters[0].appended < 1000 {
		level.Warn(util_log.Logger).Log("msg", "filter appended too few", "appended", filters[0].appended, "filter", filters[0].String(), "shard", s.shardObjectPrefix)
		return bypassAllTokens(tokens), len(tokens), nil
	}

	filterStart := time.Now()
	exists := 0
	res := make([]bool, 0, len(tokens))
	for _, token := range tokens {
		// 任意一个 filter 包含 token 即可
		contains := false
		for _, filter := range filters {
			if filter.containsAll([]string{token}) {
				exists++
				contains = true
				break
			}
		}
		res = append(res, contains)
	}
	if time.Since(filterStart) > 500*time.Millisecond {
		level.Warn(util_log.Logger).Log("msg", "filter contains slow", "duration", time.Since(filterStart), "tokens", len(tokens))
	}
	return res, exists, nil
}

// loadNewestFilter Unsafe：需要调用方加锁
// 从内存或磁盘加载最新的一个 filter，加载后会写入到 BBFShard 中
// 此时 BBFShard 中只有最后一个 filter
func (s *BBFShard) loadNewestFilter() (*bloomFilter, error) {

	// 1. 返回内存中最新的一个 filter
	if len(s.filters) > 0 {

		// filters 一般情况只有一个，因此这里排序操作应该不至于影响性能。
		// 排序是确保 filters 是有序的，才能拿到最新的 filter，如果顺序是不确定的，拿到了旧的已经写满的 filter，
		// 就会导致再创建新的 filter，进而导致 filter 数量过多
		sort.Slice(s.filters, func(i, j int) bool {
			return s.filters[i].idx < s.filters[j].idx
		})

		return s.filters[len(s.filters)-1], nil
	}

	// 2. 从磁盘中加载最新的 filter
	files, err := listDirWithPrefix(s.shardLocalPath + "_f")
	if err != nil {
		return nil, fmt.Errorf("list dir %s failed: %w", s.shardLocalPath, err)
	}
	if len(files) > 0 {
		var maxFile string
		s.maxIndex, maxFile = maxFilterFromFiles(files)

		filter, err := s.bbfStore.LoadFilterFromDisk(maxFile)
		if err != nil {
			return nil, fmt.Errorf("load filter %s failed: %w", maxFile, err)
		}

		s.filters = append(s.filters, filter)
		return filter, nil
	}

	// 3. 从对象存储加载最新的 filter
	objs, err := s.manager.metaManager.Filters(s.shardObjectPrefix)
	if err != nil {
		return nil, fmt.Errorf("list object with prefix %s failed: %w", s.shardObjectPrefix, err)
	}
	if len(objs) == 0 {
		// 4. 都没有，创建一个新的 filter
		return s.createNewFilter(), nil
	}
	var maxFilter string
	s.maxIndex, maxFilter = maxFilterFromObjectKeys(objs)
	filter, err := s.bbfStore.LoadFilter(nil, maxFilter, s.bucket, true)
	if err != nil {
		return nil, fmt.Errorf("load filter %s failed: %w", maxFilter, err)
	}
	s.filters = append(s.filters, filter)
	return filter, nil
}

// loadAllFilters 从磁盘加载所有的 filter，加载后会缓存到内存中，但不会写入到 BBFShard 中，防止被重复 Flush
func (s *BBFShard) loadAllFilters(ctx context.Context) ([]*bloomFilter, error) {
	memFilters := make([]*bloomFilter, 0, len(s.filters))

	muStart := time.Now()
	s.mutex.RLock()
	memFilters = append(memFilters, s.filters...)
	//maxIndex := s.maxIndex
	s.mutex.RUnlock()
	if time.Since(muStart) > 500*time.Millisecond {
		level.Warn(util_log.Logger).Log("msg", "read lock slow", "duration", time.Since(muStart))
	}
	if len(memFilters) > 0 {
		return memFilters, nil
	}

	// 从对象存储加载所有的 filter
	objs, err := s.manager.metaManager.Filters(s.shardObjectPrefix)
	if err != nil {
		return nil, fmt.Errorf("list object with prefix %s failed: %w", s.shardObjectPrefix, err)
	}

	loadStart := time.Now()
	filters, err := s.bbfStore.LoadFilterParallelForRead(ctx, objs, s.bucket)

	if time.Since(loadStart) > 500*time.Millisecond {
		level.Warn(util_log.Logger).Log("msg", "load filters from S3 slow", "duration", time.Since(loadStart))
	}
	return filters, nil
}

func (s *BBFShard) createNewFilter() *bloomFilter {
	s.maxIndex++
	// 在内存中创建一个新的 filter
	f := NewBloomFilter(s.manager.cfg.ShardCapacity, s.maxIndex)
	s.filters = append(s.filters, f)
	return f
}

func listDirWithPrefix(prefix string) ([]string, error) {
	files, err := filepath.Glob(prefix + "*")
	if err != nil {
		return nil, err
	}
	return files, nil
}

// buildObjectKey 生成 BBF/${name}/s${shard}_f${index} 格式的字符串
func buildObjectKey(name string, shard int, index int) string {
	return fmt.Sprintf("BBF/%s/s%d_f%d", name, shard, index)
}

func parseFileNames(file string) (string, int, int, error) {
	// file: /${path}/${name}/s${shard}_f{$index}
	parts := strings.Split(file, "/")
	if len(parts) < 2 {
		return "", 0, 0, fmt.Errorf("invalid file name: %s", file)
	}

	// Extract the name, shard, and index
	name := parts[len(parts)-2]
	shardAndIndex := parts[len(parts)-1]

	// Split shard and index
	sf := strings.Split(shardAndIndex, "_")
	if len(sf) != 2 {
		return "", 0, 0, fmt.Errorf("invalid file name: %s", file)
	}

	// Parse the shard number
	shard, err := strconv.Atoi(sf[0][1:])
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid shard number: %s", sf[0])
	}

	// Parse the index number
	index, err := strconv.Atoi(sf[1][1:])
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid index number: %s", sf[1])
	}

	return name, shard, index, nil
}

func maxFilterFromFiles(files []string) (int, string) {
	index := -1
	maxFile := ""
	for _, file := range files {
		_, _, filterIndex, err := parseFileNames(file)
		if err != nil {
			continue
		}
		if filterIndex > index {
			index = filterIndex
			maxFile = file
		}
	}
	return index, maxFile
}

func parseObjectKey(objectKey string) (string, int, int, error) {
	// objectKey: BBF/${name}/s${shard}_f{$index}
	s := strings.Split(objectKey, "/")
	if len(s) != 3 {
		return "", 0, 0, fmt.Errorf("invalid object key: %s", objectKey)
	}
	sf := strings.Split(s[2], "_")
	if len(sf) != 2 {
		return "", 0, 0, fmt.Errorf("invalid object key: %s", objectKey)
	}
	shard, err := strconv.Atoi(sf[0][1:])
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid object key: %s", objectKey)
	}
	filterIndex, err := strconv.Atoi(sf[1][1:])
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid object key: %s", objectKey)
	}
	return s[1], shard, filterIndex, nil
}

func maxFilterFromObjectKeys(keys []string) (int, string) {
	index := -1
	maxKey := ""
	for _, key := range keys {
		_, _, filterIndex, err := parseObjectKey(key)
		if err != nil {
			continue
		}
		if filterIndex > index {
			index = filterIndex
			maxKey = key
		}
	}
	return index, maxKey
}
