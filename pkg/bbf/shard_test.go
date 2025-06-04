package bbf

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/test"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/storage/config"
	"github.com/grafana/loki/v3/pkg/util/encoding"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	util_ring "github.com/grafana/loki/v3/pkg/util/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBBFShard_Add(t *testing.T) {
	m := newBBFManager(t, 2, 1)
	shard := newBBFShard(m, "test001", 0, 1000, []*bloomFilter{NewBloomFilter(m.cfg.ShardCapacity, 0)})
	chunkIds := []string{"chunk1", "chunk2", "chunk3", "chunk4", "chunk5"}
	for _, id := range chunkIds {
		done, err := shard.Add([]string{id})
		require.NoError(t, err)
		require.True(t, done)
	}
	require.Equal(t, 3, len(shard.filters))

	additionalIds := []string{"chunk5", "chunk5", "chunk5", "chunk5", "chunk6"}
	for _, id := range additionalIds {
		done, err := shard.Add([]string{id})
		require.NoError(t, err)
		require.True(t, done)
	}
	require.Equal(t, 3, len(shard.filters))
	for _, filter := range shard.filters {
		require.Equal(t, uint64(2), filter.appended)
	}
}

func TestBBFShard_AddWithSuffix(t *testing.T) {
	m := newBBFManager(t, 2, 1)
	shard := newBBFShard(m, "test001", 0, 1000, []*bloomFilter{NewBloomFilter(m.cfg.ShardCapacity, 0)})
	chunkIds := []string{"chunk1", "chunk2", "chunk3", "chunk4", "chunk5"}
	for _, id := range chunkIds {
		done, err := shard.AddWithSuffix([]string{id}, "_suffix")
		require.NoError(t, err)
		require.True(t, done)
	}
	require.Equal(t, 3, len(shard.filters))

	additionalIds := []string{"chunk5", "chunk5", "chunk5", "chunk5", "chunk6"}
	for _, id := range additionalIds {
		done, err := shard.AddWithSuffix([]string{id}, "_suffix")
		require.NoError(t, err)
		require.True(t, done)
	}
	require.Equal(t, 3, len(shard.filters))
	for _, filter := range shard.filters {
		require.Equal(t, uint64(2), filter.appended)
	}
}

func TestBBFManager(t *testing.T) {
	bbfshards := 10
	m := newBBFManager(t, 2000, bbfshards)
	tokens := make(map[string]struct{})
	for i := 0; i < 1000; i++ {
		tokens["chunkId"+strconv.Itoa(i)] = struct{}{}
	}

	rs, err := m.rm.Ring.GetReplicationSetForOperation(ring.Read)
	require.NoError(t, err)
	chk := logproto.ChunkRef{UserID: "fake", Through: model.Time(1000)}
	reqs := buildAddRequests(tokens, rs.Instances, config.PeriodConfig{BBFShards: bbfshards}, chk)
	for _, req := range reqs {
		_, err := m.Add(context.Background(), req)
		require.NoError(t, err)
	}
	filter := buildChunkBloomFilterKey(&chk)
	period, err := m.schema.SchemaForTime(chk.Through)
	require.NoError(t, err)

	for i := 0; i < 1000; i++ {
		token := "chunkId" + strconv.Itoa(i)
		shard := int(encoding.XxhashHashIndex(StringToBytes(token), period.BBFShards))
		req := &BBFExistsRequest{
			Filter: filter,
			Value:  token,
			Refs:   []*logproto.ChunkRef{&chk},
			Shard:  int32(shard),
		}

		rsp, err := m.BBFExists(context.Background(), req)
		require.NoError(t, err)
		require.True(t, rsp.Exists[0])
	}
}

func TestBBFManager_ErrorRate(t *testing.T) {
	var (
		bbfshards = 1
		num       = 2000_000
	)
	m := newBBFManager(t, 2*num/bbfshards, bbfshards)
	tokens := make(map[string]struct{})
	for i := 0; i < num; i++ {
		tokens["chunkId"+strconv.Itoa(i)] = struct{}{}
	}

	rs, err := m.rm.Ring.GetReplicationSetForOperation(ring.Read)
	require.NoError(t, err)
	chk := logproto.ChunkRef{UserID: "fake", Through: model.Time(1000), From: model.Time(100), Checksum: uint32(439)}
	reqs := buildAddRequests(tokens, rs.Instances, config.PeriodConfig{BBFShards: bbfshards}, chk)
	for _, req := range reqs {
		_, err := m.Add(context.Background(), req)
		require.NoError(t, err)
	}
	filter := buildChunkBloomFilterKey(&chk)
	period, err := m.schema.SchemaForTime(chk.Through)
	require.NoError(t, err)

	for i := 0; i < num; i++ {
		token := "chunkId" + strconv.Itoa(i)
		shard := int(encoding.XxhashHashIndex(StringToBytes(token), period.BBFShards))
		req := &BBFExistsRequest{
			Filter: filter,
			Value:  token,
			Refs:   []*logproto.ChunkRef{&chk},
			Shard:  int32(shard),
		}

		rsp, err := m.BBFExists(context.Background(), req)
		require.NoError(t, err)
		require.True(t, rsp.Exists[0])
	}

	// test error rate
	falsePositives := 0
	for i := 0; i < num; i++ {
		token := "will_not_exist_long_id_" + strconv.Itoa(i)
		shard := int(encoding.XxhashHashIndex(StringToBytes(token), period.BBFShards))
		req := &BBFExistsRequest{
			Filter: filter,
			Value:  token,
			Refs:   []*logproto.ChunkRef{&chk},
			Shard:  int32(shard),
		}

		rsp, err := m.BBFExists(context.Background(), req)
		require.NoError(t, err)
		if rsp.Exists[0] {
			falsePositives++
		}
	}
	errorRate := float64(falsePositives) / float64(num)
	t.Logf("Error rate: %f, falsePositives: %d", errorRate, falsePositives)

	// 验证错误率低于 0.001
	require.LessOrEqualf(t, errorRate, 0.001, "Error rate is lower than expected")
}

func TestLoadNewestFilter(t *testing.T) {
	os.MkdirAll("/tmp/202409110300", 0755)
	f0 := "/tmp/202409110300/s616_f0"
	f1 := "/tmp/202409110300/s616_f1"
	_, err := os.OpenFile(f0, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0755)
	require.NoError(t, err)
	_, err = os.OpenFile(f1, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0755)
	require.NoError(t, err)
	defer os.Remove(f0)
	defer os.Remove(f1)

	files, err := listDirWithPrefix("/tmp/202409110300/s616_f")
	require.NoError(t, err)
	require.Equal(t, 2, len(files))

	var maxFile string
	maxIndex, maxFile := maxFilterFromFiles(files)
	assert.Equal(t, 1, maxIndex)
	assert.Equal(t, "/tmp/202409110300/s616_f1", maxFile)

}

func TestFilter(t *testing.T) {
	filterFile := "/Users/honganan/s1539_f0"
	filter, err := loadFilterFromDisk(filterFile)
	require.NoError(t, err)
	t.Logf("filter: %v", filter)
}

// loadFilterFromDisk 从磁盘加载 filter
func loadFilterFromDisk(filterName string) (*bloomFilter, error) {
	buf, err := os.ReadFile(filterName)
	if err != nil {
		return nil, fmt.Errorf("read file %s failed: %w", filterName, err)
	}
	filter := getBloomFilter()
	if err := filter.unmarshal(buf); err != nil {
		return nil, fmt.Errorf("unmarshal filter %s failed: %w", filterName, err)
	}
	return filter, nil
}

//func TestParseChunkId(t *testing.T) {
//	chunkId := "fake/af0b8231895d1a6/191daaf4273:191dab407b0:77ff500c"
//	chk, err := parseChunkKey(chunkId)
//	require.NoError(t, err)
//	filter := bloomBucket(chk.Through.Time())
//	t.Logf("filter: %s", filter)
//	t.Logf("chunk: %v", chk)
//}

func newBBFManager(t *testing.T, capacity int, bbfShards int) *BBFManager {
	ringCfg := util_ring.RingConfig{
		KVStore: kv.Config{
			Store: "inmemory",
			//Mock: kvStore,
		},
		HeartbeatPeriod:  60 * time.Second,
		HeartbeatTimeout: 60 * time.Second,
	}

	cfg := Config{
		ShardCapacity:     capacity,
		WindowSize:        5,
		FlushInterval:     1 * time.Minute,
		IdleFlushDuration: 10 * time.Minute,
		GcInterval:        1 * time.Hour,
		Retention:         1 * time.Hour,
		WalPath:           "/tmp",
		RingConfig:        ringCfg,
	}
	period := config.PeriodConfig{
		From:        config.NewDayTime(model.Time(0)),
		IndexTables: config.IndexPeriodicTableConfig{},
		ChunkTables: config.PeriodicTableConfig{},
		BBFShards:   bbfShards,
	}

	rm, err := util_ring.NewRingManager("bbf-index", util_ring.ServerMode, ringCfg, 1, 1, util_log.Logger, prometheus.DefaultRegisterer)
	require.NoError(t, err)
	require.NoError(t, rm.StartAsync(context.Background()))

	test.Poll(t, 2*time.Second, 1, func() interface{} {
		rs, _ := rm.Ring.GetAllHealthy(ring.Read)
		return len(rs.Instances)
	})
	m, err := NewBBFManger(cfg, []config.PeriodConfig{period}, &mockStore{}, rm, prometheus.DefaultRegisterer, util_log.Logger)
	require.NoError(t, err)
	return m
}
