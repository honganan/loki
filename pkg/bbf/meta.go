package bbf

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/prometheus/common/model"
)

const (
	delayBuf          = 2 * time.Hour
	defaultMetasCycle = 10 // 10 minutes

	refreshInterval    = 10 * time.Minute
	createFileInterval = 12 * time.Hour
	deleteInterval     = 10 * time.Minute
)

type bucketOpt struct {
	bucket model.Time
	prefix string
}

var bucketOptPool = sync.Pool{
	New: func() interface{} {
		return &bucketOpt{}
	},
}

func getBucketOpt() *bucketOpt {
	return bucketOptPool.Get().(*bucketOpt)
}

func putBucketOpt(b *bucketOpt) {
	if b == nil {
		return
	}
	b.bucket = 0
	b.prefix = ""
	bucketOptPool.Put(b)
}

type PrefixTypeOpt interface {
	MetasPrefix() []*bucketOpt
	ListPrefix() []*bucketOpt
}

// todo: 优化
type refreshTime struct {
	days    int
	hours   int
	minutes int

	startTime time.Time
}

func NewRefreshTime(days, hours, minutes int, now time.Time) PrefixTypeOpt {
	re := &refreshTime{days: days, hours: hours, minutes: minutes, startTime: now}
	re.parseToday()
	return re
}

func (t *refreshTime) parseToday() {
	if t.days > 0 {
		t.days--
		// 今天的时间直接list object
		t.hours += t.startTime.Hour()
		t.minutes += t.startTime.Minute()
	}
}

func (t *refreshTime) MetasPrefix() []*bucketOpt {
	pre := make([]*bucketOpt, 0, t.days)
	if t.days == 0 {
		return pre
	}

	for i := 1; i <= t.days; i++ {
		day := t.startTime.AddDate(0, 0, -i)
		dayPrefix := day.Format("20060102")
		bucket := bloomBucketTime(model.TimeFromUnix(day.Unix()))

		b := getBucketOpt()
		b.bucket = bucket
		b.prefix = fmt.Sprintf("BBF/meta_%s", dayPrefix)
		pre = append(pre, b)
	}

	return pre
}

func (t *refreshTime) ListPrefix() []*bucketOpt {
	pre := make([]*bucketOpt, 0)
	minutes := t.minutes
	if t.hours > 0 {
		minutes += t.hours * 60
	}

	// 10分钟一个文件
	for i := minutes; i > 0; i -= defaultMetasCycle {
		prefix := bloomBucket(t.startTime.Add(-time.Minute * time.Duration(i)))
		bucket := bloomBucketTime(model.TimeFromUnix(t.startTime.Add(-time.Minute * time.Duration(i)).Unix()))

		b := getBucketOpt()
		b.bucket = bucket
		b.prefix = fmt.Sprintf("BBF/%s", prefix)
		pre = append(pre, b)
	}

	return pre
}

type Metas struct {
	mu sync.Mutex

	data map[string]map[int][]int
}

func NewMetas() *Metas {
	return &Metas{
		data: make(map[string]map[int][]int),
	}
}

func (m *Metas) getShard(name string) map[int][]int {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.data[name]; !ok {
		m.data[name] = make(map[int][]int)
	}
	return m.data[name]
}

func (m *Metas) setShard(name string, shard map[int][]int) {
	m.mu.Lock()
	m.data[name] = shard
	m.mu.Unlock()
}

func (m *Metas) addObject(contents []string) {
	for _, object := range contents {
		name, s, f, _ := parseObjectKey(object)
		shard := m.getShard(name)
		shard[f] = append(shard[f], s)
		m.setShard(name, shard)
	}
}

func (m *Metas) combineMetas(metas []byte) {
	metas2 := Metas{
		data: make(map[string]map[int][]int),
	}
	_ = json.Unmarshal(metas, &metas2)

	for k, v := range metas2.data {
		if _, ok := m.data[k]; !ok {
			m.data[k] = make(map[int][]int)
		}

		for idx, shards := range v {
			for _, s := range shards {
				m.data[k][idx] = append(m.data[k][idx], s)
			}
		}
	}
}

func (m *Metas) toMap() (map[string][]string, int) {
	var (
		res   = make(map[string][]string)
		count = 0
	)

	// map[string]map[int][]int
	for bbfName, data := range m.data {
		for idx, shards := range data {
			for _, s := range shards {
				shardObjPrefix := fmt.Sprintf("BBF/%s/s%d", bbfName, s)
				res[shardObjPrefix] = append(res[shardObjPrefix], buildObjectKey(bbfName, s, idx))
				count++
			}
		}
	}
	return res, count
}

func (m *Metas) MarshalJSON() ([]byte, error) {
	if m.data == nil {
		m.data = make(map[string]map[int][]int)
	}
	return json.Marshal(m.data)
}

func (m *Metas) UnmarshalJSON(bytes []byte) error {
	if m.data == nil {
		m.data = make(map[string]map[int][]int)
	}
	return json.Unmarshal(bytes, &m.data)
}

type MetaManager struct {
	ctx context.Context

	metas    map[string][]string
	bbfStore Store

	mu sync.RWMutex
}

func NewMetaManager(ctx context.Context, bbfStore Store) *MetaManager {
	mm := &MetaManager{
		ctx:      ctx,
		metas:    make(map[string][]string),
		bbfStore: bbfStore,
	}

	mm.init()
	return mm
}

func (mm *MetaManager) Filters(shardObjPrefix string) ([]string, error) {
	mm.mu.RLock()
	filters, ok := mm.metas[shardObjPrefix]
	mm.mu.RUnlock()
	if ok {
		return filters, nil
	}
	return nil, nil
}

func (mm *MetaManager) AddFilter(shardObjPrefix, filter string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	fs, ok := mm.metas[shardObjPrefix]
	if !ok {
		fs = make([]string, 0, 1)
	}

	filterSet := make(map[string]struct{}, len(fs))
	for _, f := range fs {
		filterSet[f] = struct{}{}
	}

	if _, exists := filterSet[filter]; !exists {
		fs = append(fs, filter)
	}

	mm.metas[shardObjPrefix] = fs
}

func (mm *MetaManager) init() {
	for i := 1; i < 7; i++ {
		// todo: 使用ring
		time.Sleep(time.Millisecond * time.Duration(rand.Uint64N(500)))
		mm.createMetaFile(i)
	}

	// 启动时先初始化 7 天的元数据，后面定时刷新最近的元数据
	mm.refreshMeta(NewRefreshTime(7, 0, 0, time.Now()))

	go mm.loopDeleteExpiredMeta()
	go mm.loopRefreshMeta()
	go mm.loopCreateMetaFile()
}

func (mm *MetaManager) loopDeleteExpiredMeta() {
	ticker := time.NewTicker(deleteInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = level.Info(util_log.Logger).Log("msg", "delete mm.metas data...")
			mm.deleteExpiredMeta()

		case <-mm.ctx.Done():
			return
		}
	}
}

func (mm *MetaManager) loopRefreshMeta() {
	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = level.Info(util_log.Logger).Log("msg", "refreshing meta...")
			mm.refreshMeta(NewRefreshTime(0, 3, 0, time.Now()))
		case <-mm.ctx.Done():
			return
		}
	}
}

func (mm *MetaManager) loopCreateMetaFile() {
	ticker := time.NewTicker(createFileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = level.Info(util_log.Logger).Log("msg", "generating meta file...")
			// 减少多结点重复创建
			time.Sleep(time.Second * time.Duration(rand.Uint64N(60)))
			for i := 1; i <= 2; i++ {
				mm.createMetaFile(i)
			}
		case <-mm.ctx.Done():
			return
		}
	}
}

func (mm *MetaManager) refreshMeta(t PrefixTypeOpt) {
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		for _, b := range t.MetasPrefix() {
			mm.refreshByMetaFile(b)
		}
		wg.Done()
	}()

	go func() {
		// todo: 并发
		for _, b := range t.ListPrefix() {
			mm.refreshByListFile(b)
		}
		wg.Done()
	}()

	wg.Wait()
}

func (mm *MetaManager) refreshByMetaFile(b *bucketOpt) {
	start := time.Now()
	objs, err := mm.bbfStore.GetObject(mm.ctx, b.prefix, b.bucket)
	if err != nil {
		_ = level.Error(util_log.Logger).Log("msg", "failed to get meta objects", "error", err)

		// 如果获取失败，用list兜底
		mm.refreshByListFile(b)
		return
	}

	metas := NewMetas()
	_ = json.Unmarshal(objs, &metas)
	m, count := metas.toMap()
	mm.mu.Lock()
	for k, data := range m {
		mm.metas[k] = data
	}
	mm.mu.Unlock()
	_ = level.Info(util_log.Logger).Log("msg", "refresh meta", "type", "meta file", "prefix", b.prefix, "count", count, "duration", time.Since(start))
	putBucketOpt(b)
}

// refresh 刷新最近 daysAgo 天的数据文件, 0 表示当天
func (mm *MetaManager) refreshByListFile(b *bucketOpt) {
	start := time.Now()
	objs, err := mm.bbfStore.ListObjectWithPrefix(mm.ctx, b.prefix, b.bucket)
	if err != nil {
		_ = level.Error(util_log.Logger).Log("msg", "failed to list objects", "error", err)
		return
	}

	shardObjs := make(map[string][]string, len(objs))
	for _, obj := range objs {
		bbfName, s, _, err := parseObjectKey(obj)
		if err != nil {
			_ = level.Error(util_log.Logger).Log("msg", "failed to parse object key", "error", err)
			continue
		}
		key := fmt.Sprintf("BBF/%s/s%d", bbfName, s)
		shardObjs[key] = append(shardObjs[key], obj)
	}

	mm.mu.Lock()
	for k, v := range shardObjs {
		mm.metas[k] = v
	}
	mm.mu.Unlock()
	_ = level.Info(util_log.Logger).Log("msg", "refresh meta", "type", "data file", "prefix", b.prefix, "count", len(objs), "duration", time.Since(start))
	putBucketOpt(b)
}

func (mm *MetaManager) createMetaFile(daysAgo int) bool {
	nowSubBuf := time.Now().Add(-delayBuf)
	day := nowSubBuf.AddDate(0, 0, -daysAgo)
	dayPrefix := day.Format("20060102")
	bucket := bloomBucketTime(model.TimeFromUnix(day.Unix()))

	_, err := mm.bbfStore.GetObject(mm.ctx, fmt.Sprintf("BBF/meta_%s", dayPrefix), bucket)
	if err == nil {
		// 如果文件存在，不再生成
		return false
	}

	metas := NewMetas()
	objs, err := mm.bbfStore.ListObjectWithPrefix(mm.ctx, fmt.Sprintf("BBF/%s", dayPrefix), bucket)
	if err != nil {
		_ = level.Error(util_log.Logger).Log("msg", "failed to list objects", "error", err)
		return false
	}
	metas.addObject(objs)
	data, err := json.Marshal(metas)
	if err != nil {
		_ = level.Error(util_log.Logger).Log("msg", "failed to marshal metas", "error", err)
		return false
	}

	err = mm.bbfStore.PutObject(mm.ctx, fmt.Sprintf("BBF/meta_%s", dayPrefix), data, bucket)
	if err != nil {
		_ = level.Error(util_log.Logger).Log("msg", "failed to put object", "error", err)
		return false
	}

	return true
}

func (mm *MetaManager) deleteExpiredMeta() {
	deleteTime := time.Now().AddDate(0, 0, -8)
	deleteTimeStr := deleteTime.Format("200601021504")

	deleteKeys := make([]string, 0, 3000)
	metas := mm.getMetas()
	for k := range metas {
		parts := strings.Split(k, "/")
		if len(parts) < 3 {
			continue // 跳过无效路径
		}
		bbfName := parts[1]
		if deleteTimeStr > bbfName {
			deleteKeys = append(deleteKeys, k)
		}
	}

	mm.mu.Lock()
	for _, k := range deleteKeys {
		delete(mm.metas, k)
	}
	mm.mu.Unlock()
}

func (mm *MetaManager) getMetas() map[string][]string {
	metas := make(map[string][]string, len(mm.metas))
	mm.mu.RLock()
	for k, v := range mm.metas {
		metas[k] = v
	}
	mm.mu.RUnlock()

	return metas
}

func (mm *MetaManager) setMetas(metas map[string][]string) {
	mm.mu.Lock()
	mm.metas = metas
	mm.mu.Unlock()
}
