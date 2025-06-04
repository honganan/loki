package bbf

import (
	"context"
	"slices"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/indexgateway"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/querier/plan"
	"github.com/grafana/loki/v3/pkg/storage/config"
	"github.com/grafana/loki/v3/pkg/util/encoding"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

type BBFQuerier struct {
	pool      *Pool
	ring      ring.ReadRing
	schema    config.SchemaConfig
	bbfConfig Config
	logger    log.Logger
}

func NewBBFQuerier(pool *Pool, ring ring.ReadRing, bbfConfig Config, schema config.SchemaConfig, logger log.Logger) indexgateway.BloomQuerier {
	return &BBFQuerier{
		pool:      pool,
		ring:      ring,
		schema:    schema,
		bbfConfig: bbfConfig,
		logger:    logger,
	}
}

func (bq *BBFQuerier) FilterChunkRefs(
	_ context.Context,
	tenant string,
	_, through model.Time,
	_ map[uint64]labels.Labels,
	chunkRefs []*logproto.ChunkRef,
	queryPlan plan.QueryPlan,
) ([]*logproto.ChunkRef, bool, error) {
	if _, ok := bq.bbfConfig.EnabledTenantsMap[tenant]; !ok {
		return chunkRefs, false, nil
	}
	schema, err := bq.schema.SchemaForTime(through)
	if err != nil {
		return nil, false, err
	}
	//schema.BBFStreams
	if !MatchBloomStreams(schema, LabelPairsFromExpr(queryPlan.AST)) {
		return chunkRefs, false, nil
	}

	// Shortcut that does not require any filtering
	exprs := ExtractTestableLineFilters(queryPlan.AST)
	if len(chunkRefs) == 0 || len(exprs) == 0 {
		return chunkRefs, false, nil
	}

	rs, err := bq.ring.GetReplicationSetForOperation(ring.Read)
	if err != nil {
		return nil, false, err
	}
	instances := rs.Instances
	slices.SortFunc(instances, func(i, j ring.InstanceDesc) int {
		return strings.Compare(i.Addr, j.Addr)
	})
	checker := newChkChecker(tenant, chunkRefs, bq.pool, instances, bq.schema)

	bloomTest := FiltersToBloomTest(&SimpleSplitTokenizer{}, exprs...)
	refs := bloomTest.Matches(checker)

	// TODO(anan): 应该不需要排序，查询器从 index-gateway 查询后转为 LazyChunks 放到 batchChunkIterator 中会排序
	return refs, true, nil
}

func MatchBloomStreams(schema config.PeriodConfig, labelPairs labels.Labels) bool {
	for _, _stream := range schema.BBFStreams {
		if labels.Selector(_stream.Matchers).Matches(labelPairs) {
			return true
		}
	}
	return false
}

func LabelPairsFromExpr(expr syntax.Expr) labels.Labels {
	var lbs labels.Labels
	expr.Walk(func(e syntax.Expr) bool {
		if e1, ok := e.(*syntax.MatchersExpr); ok {
			for _, m := range e1.Matchers() {
				lbs = append(lbs, labels.Label{
					Name:  m.Name,
					Value: m.Value,
				})
			}
		}
		return true // continue walking
	})
	return lbs
}

type chkChecker struct {
	pool      *Pool
	instances []ring.InstanceDesc
	schema    config.SchemaConfig

	tenant string
	refs   []*logproto.ChunkRef
}

func newChkChecker(tenant string, refs []*logproto.ChunkRef, pool *Pool, instances []ring.InstanceDesc, schema config.SchemaConfig) *chkChecker {
	return &chkChecker{
		refs:      refs,
		pool:      pool,
		schema:    schema,
		tenant:    tenant,
		instances: instances,
	}
}

func (lc *chkChecker) Test(token string) []*logproto.ChunkRef {
	var (
		groups = groupChunksByTimeBucket(lc.refs)
		keys   = filterKeys(groups)
		result = make([]*logproto.ChunkRef, 0, len(lc.refs)/10)
		lock   sync.Mutex
	)

	err := concurrency.ForEachJob(context.Background(), len(groups), len(groups), func(ctx context.Context, i int) error {
		filter := keys[i]
		req := &BBFExistsRequest{
			Filter: filter,
			Value:  token,
			Refs:   groups[filter],
		}

		period, err := lc.schema.SchemaForTime(req.Refs[0].Through)
		if err != nil {
			return err
		}
		// 计算 token 的 shard，计算对应客户端拿到 client，并进行请求
		shard := int(encoding.XxhashHashIndex(StringToBytes(token), period.BBFShards))
		req.Shard = int32(shard)

		index := shard % len(lc.instances)
		addr := lc.instances[index].Addr
		client, err := lc.pool.GetClientFor(addr)
		if err != nil {
			return err
		}

		ctx = user.InjectOrgID(ctx, lc.tenant)
		rsp, err := client.(BBFClient).BBFExists(ctx, req)
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "failed to test bloom filter", "err", err)
			lock.Lock()
			result = append(result, groups[filter]...)
			lock.Unlock()
			return nil
		}
		if len(rsp.Exists) != len(groups[filter]) {
			return errors.New("mismatched number of requests chunks and response")
		}

		lock.Lock()
		for j, exist := range rsp.Exists {
			if exist {
				result = append(result, groups[filter][j])
			}
		}
		lock.Unlock()
		return nil
	})
	if err != nil {
		_ = level.Error(util_log.Logger).Log("msg", "failed to test bloom filter", "err", err)
	}
	return result
}

func groupChunksByTimeBucket(refs []*logproto.ChunkRef) map[string][]*logproto.ChunkRef {
	groups := make(map[string][]*logproto.ChunkRef)
	for _, ref := range refs {
		filterName := buildChunkBloomFilterKey(ref)
		groups[filterName] = append(groups[filterName], ref)
	}
	return groups
}

func filterKeys(m map[string][]*logproto.ChunkRef) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// intersectChunks 计算多个 logproto.ChunkRef 数组的交集。
func intersectChunks(results [][]*logproto.ChunkRef) []*logproto.ChunkRef {
	if len(results) == 0 {
		return nil
	}
	if len(results) == 1 {
		return results[0]
	}

	chunkCount := make(map[logproto.ChunkRef]int)
	for _, chunks := range results {
		for _, chunk := range chunks {
			chunkCount[*chunk]++
		}
	}

	var intersection []*logproto.ChunkRef
	for chunk, count := range chunkCount {
		if count == len(results) {
			chunkCopy := chunk // 创建一个副本以获取指针
			intersection = append(intersection, &chunkCopy)
		}
	}

	return intersection
}

// unionChunks 计算多个 logproto.ChunkRef 数组的并集。
func unionChunks(results [][]*logproto.ChunkRef) []*logproto.ChunkRef {
	if len(results) == 0 {
		return nil
	}
	if len(results) == 1 {
		return results[0]
	}

	chunkSet := make(map[logproto.ChunkRef]struct{})
	for _, chunks := range results {
		for _, chunk := range chunks {
			chunkSet[*chunk] = struct{}{}
		}
	}

	var union []*logproto.ChunkRef
	for chunk := range chunkSet {
		chunkCopy := chunk // 创建一个副本以获取指针
		union = append(union, &chunkCopy)
	}

	return union
}
