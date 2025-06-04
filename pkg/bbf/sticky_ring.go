package bbf

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/pkg/errors"
)

// StickyRing 有粘性的 Ring：可以实现当节点数发生变化时，保证相同时间 bucket 的节点列表分配不变，
// 保持相同节点分配的方法是：根据时间戳生成一个 bucket，然后将 bucket 与节点分配的映射缓存到本地
type StickyRing struct {
	cache   simplelru.LRUCache[string, ring.ReplicationSet]
	cacheMu sync.RWMutex

	kvClient kv.Client
	ring     *ring.Ring
}

func NewStickyRing(cacheSize int, kvClient kv.Client, r *ring.Ring) (*StickyRing, error) {
	cache, err := simplelru.NewLRU[string, ring.ReplicationSet](cacheSize, nil)
	if err != nil {
		return nil, err
	}
	s := &StickyRing{
		cache:    cache,
		kvClient: kvClient,
		ring:     r,
	}
	go s.gc()
	return s, nil
}

// gc 定时清理过期的 key，key的规则应该是时间开头的字符串，例如 20220101...
// 注意！：如果 key 是以租户开头会有问题
func (s *StickyRing) gc() {
	// 定时清理过期的 key
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			deleted := 0
			keys, err := s.kvClient.List(context.Background(), "")
			if err != nil {
				level.Warn(util_log.Logger).Log("msg", "failed to list keys", "error", err)
			}
			earliest := time.Now().Add(-24 * time.Hour).Format("20060102")
			for _, key := range keys {
				if key < earliest {
					if err := s.kvClient.Delete(context.Background(), key); err != nil {
						level.Warn(util_log.Logger).Log("msg", "failed to delete key", "key", key, "error", err)
					}
					deleted++
				}
			}
			level.Info(util_log.Logger).Log("msg", "KV store garbage collected", "deleted", deleted, "total", len(keys))
		}
	}

}

// StickyReplicationSetForOperation 根据时间 bucket 获取节点列表
// 节点列表会缓存在 LRU 缓存中，并写入到 kvClient 里，这里缓存并没有考虑 Operation，如果有
// 不同的 Operation 需要不同的节点列表，需要重构缓存逻辑
// TODO(anan): 要再推敲下
func (s *StickyRing) StickyReplicationSetForOperation(op ring.Operation, bucket string) (ring.ReplicationSet, error) {

	// From local cache
	s.cacheMu.RLock()
	rs, ok := s.cache.Get(bucket)
	s.cacheMu.RUnlock()
	if ok {
		return rs, nil
	}

	// From KV store
	v, err := s.kvClient.Get(context.Background(), bucket)
	if err != nil {
		return ring.ReplicationSet{}, err
	}
	if v != nil {
		replications := v.([]ring.InstanceDesc)
		return ring.ReplicationSet{Instances: replications}, nil
	}

	// Read and cache&write
	instances, err := s.ring.GetReplicationSetForOperation(op)
	if err != nil {
		return ring.ReplicationSet{}, err
	}
	err = s.kvClient.CAS(context.Background(), bucket, func(in interface{}) (out interface{}, retry bool, err error) {
		_, ok := in.([]ring.InstanceDesc)
		if !ok {
			return in, false, errors.New("invalid value in kv store")
		}

		return instances.Instances, false, nil
	})

	s.cacheMu.Lock()
	s.cache.Add(bucket, instances)
	s.cacheMu.Unlock()
	return instances, nil
}
