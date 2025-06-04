package bbf

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/loki/v3/pkg/bytesutil"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/prometheus/common/model"
	"golang.org/x/exp/slices"
)

type BBF struct {
	mu      sync.Mutex
	manager *BBFManager

	Name        string            `json:"name"`
	Dir         string            `json:"-"`
	CreateTime  int64             `json:"createTime"`
	LastUpdated int64             `json:"lastUpdated"`
	Bucket      model.Time        `json:"-"`
	Shards      map[int]*BBFShard `json:"-"`
}

func NewBBF(name string, bucket model.Time, manager *BBFManager) (*BBF, error) {
	bbf := &BBF{
		manager:     manager,
		Name:        name,
		Dir:         filepath.Join(manager.cfg.Path, name),
		Bucket:      bucket,
		Shards:      make(map[int]*BBFShard, 1000),
		CreateTime:  time.Now().Unix(),
		LastUpdated: time.Now().Unix(),
	}

	rs, err := manager.stickyRing.StickyReplicationSetForOperation(ring.Read, bloomBucket(bucket.Time()))
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "failed to get sticky replication set", "err", err)
		return nil, err
	}
	schema, err := manager.schema.SchemaForTime(bucket)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "failed to get schema for time", "err", err)
		return nil, err
	}
	instances := rs.Instances
	slices.SortFunc(instances, func(i, j ring.InstanceDesc) int {
		return strings.Compare(i.Addr, j.Addr)
	})

	myselfIndex := -1
	for index, instance := range instances {
		if instance.Addr == manager.rm.RingLifecycler.GetInstanceAddr() {
			myselfIndex = index
		}
	}
	if myselfIndex == -1 {
		level.Error(util_log.Logger).Log("msg", "failed to get myself index in instances", "len(instances)", len(instances), "myself", manager.rm.RingLifecycler.GetInstanceAddr())
		return nil, errors.New("failed to get myself index in instances")
	}

	// 初始化属于当前节点的所有分片
	err = concurrency.ForEachJob(manager.ctx, schema.BBFShards, 100, func(ctx context.Context, s int) error {
		i := s % len(instances)
		// 只有一个实例时是例外情况，不能用求余的方法判断
		if i == myselfIndex || len(instances) == 1 {

			filters := make([]*bloomFilter, 0, 1)

			shardObjPrefix := fmt.Sprintf("BBF/%s/s%d", name, s)
			fs, err := manager.metaManager.Filters(shardObjPrefix)
			if err != nil {
				level.Error(util_log.Logger).Log("msg", "failed to get filters", "err", err)
				return err
			}

			// 对 filters 排序，确保写入时直接获取 s.filters[len(s.filters)-1] 是拿到最新的 filter
			sort.Strings(fs)
			for _, f := range fs {
				bf, err := manager.bbfStore.LoadFilter(manager.ctx, f, bucket, true)
				if err != nil {
					if errors.Is(err, EmptyFilterObjError) {
						level.Warn(util_log.Logger).Log("msg", "load filter found empty object, will create new one instead", "filter", f)
						continue
					} else {
						level.Error(util_log.Logger).Log("msg", "failed to load filter", "filter", f, "err", err)
						return err
					}
				}
				filters = append(filters, bf)
			}

			if len(filters) == 0 {
				if s%100 == 0 {
					level.Info(util_log.Logger).Log("msg", "create new filter", "name", name, "shard", s)
				}
				filters = append(filters, NewBloomFilter(manager.cfg.ShardCapacity, 0))
			}
			bbf.mu.Lock()
			bbf.Shards[s] = newBBFShard(manager, name, s, bucket, filters)
			bbf.mu.Unlock()
		}
		return nil
	})
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "failed to create owned shard", "err", err)
		return nil, err
	}

	var shardKeys []string
	for shardKey := range bbf.Shards {
		shardKeys = append(shardKeys, strconv.Itoa(shardKey))
	}
	shardKeysStr := strings.Join(shardKeys, ", ")
	level.Info(util_log.Logger).Log("msg", "new bbf created", "name", name, "len(shards)", len(bbf.Shards), "shards", shardKeysStr)
	return bbf, nil
}

// getShard 需要在 new BBFManager 的时候初始化所有 owned shards，并创建出 filter，否则会报错
// 如果在这里初始化 shard 可能会导致 Add 时对逐个 shard 去 s3 list，影响性能
func (bf *BBF) getShard(shard int) (*BBFShard, error) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	s, ok := bf.Shards[shard]
	if !ok {
		return nil, fmt.Errorf("shard %d not found, name: %s", shard, bf.Name)
	}
	return s, nil
}

func (bf *BBF) AddTo(shard int, tokens []string, suffix string) error {
	s, err := bf.getShard(shard)
	if err != nil {
		return err
	}

	if len(suffix) == 0 {
		_, err := s.Add(tokens)
		return err
	}

	ok, err := s.Add(tokens)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("add to shard %d failed", shard)
	}

	ok, err = s.AddWithSuffix(tokens, suffix)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("add to shard %d failed", shard)
	}
	bf.LastUpdated = time.Now().Unix()
	return nil
}

func (bf *BBF) calculateShard(token string) *BBFShard {
	schema, err := bf.manager.schema.SchemaForTime(model.Now())
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "failed to get schema for time", "err", err)
		return nil
	}
	idx := xxhash.Sum64(bytesutil.ToUnsafeBytes(token)) % uint64(schema.BBFShards)
	return bf.Shards[int(idx)]
}

func (bf *BBF) reset() {
	bf.manager = nil
	bf.Name = ""
	bf.Dir = ""
	bf.Shards = make(map[int]*BBFShard)
	bf.CreateTime = 0
	bf.LastUpdated = 0
}

// ShouldFlush bbf should be flushed after `flushAfter` time
func (bf *BBF) ShouldFlush() bool {
	return bf.LastUpdated+int64(bf.manager.cfg.IdleFlushDuration.Seconds()) < time.Now().Unix()
}

func (bf *BBF) getNeedFlushShards() []*BBFShard {
	var shards []*BBFShard
	for _, shard := range bf.Shards {
		if shard.needFlush() {
			shards = append(shards, shard)
		}
	}
	return shards
}

func (bf *BBF) filtersCount() int {
	count := 0
	for _, shard := range bf.Shards {
		count += len(shard.filters)
	}
	return count
}
