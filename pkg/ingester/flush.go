package ingester

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/bbf"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/storage/config"
	"github.com/grafana/loki/v3/pkg/util/encoding"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/exp/slices"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"

	"github.com/grafana/loki/v3/pkg/chunkenc"
	"github.com/grafana/loki/v3/pkg/storage/chunk"
	"github.com/grafana/loki/v3/pkg/util"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
)

const (
	// Backoff for retrying 'immediate' flushes. Only counts for queue
	// position, not wallclock time.
	flushBackoff = 1 * time.Second

	// Lower bound on flushes per check period for rate-limiter
	minFlushes = 100

	nameLabel = "__name__"
	logsValue = "logs"

	flushReasonIdle     = "idle"
	flushReasonMaxAge   = "max_age"
	flushReasonForced   = "forced"
	flushReasonNotOwned = "not_owned"
	flushReasonFull     = "full"
	flushReasonSynced   = "synced"
)

// I don't know if this needs to be private but I only needed it in this package.
type flushReasonCounter struct {
	flushReasonIdle     int
	flushReasonMaxAge   int
	flushReasonForced   int
	flushReasonNotOwned int
	flushReasonFull     int
	flushReasonSynced   int
}

func (f *flushReasonCounter) Log() []interface{} {
	// return counters only if they are non zero
	var log []interface{}
	if f.flushReasonIdle > 0 {
		log = append(log, "idle", f.flushReasonIdle)
	}
	if f.flushReasonMaxAge > 0 {
		log = append(log, "max_age", f.flushReasonMaxAge)
	}
	if f.flushReasonForced > 0 {
		log = append(log, "forced", f.flushReasonForced)
	}
	if f.flushReasonNotOwned > 0 {
		log = append(log, "not_owned", f.flushReasonNotOwned)
	}
	if f.flushReasonFull > 0 {
		log = append(log, "full", f.flushReasonFull)
	}
	if f.flushReasonSynced > 0 {
		log = append(log, "synced", f.flushReasonSynced)
	}
	return log
}

func (f *flushReasonCounter) IncrementForReason(reason string) error {
	switch reason {
	case flushReasonIdle:
		f.flushReasonIdle++
	case flushReasonMaxAge:
		f.flushReasonMaxAge++
	case flushReasonForced:
		f.flushReasonForced++
	case flushReasonNotOwned:
		f.flushReasonNotOwned++
	case flushReasonFull:
		f.flushReasonFull++
	case flushReasonSynced:
		f.flushReasonSynced++
	default:
		return fmt.Errorf("unknown reason: %s", reason)
	}
	return nil
}

// Note: this is called both during the WAL replay (zero or more times)
// and then after replay as well.
func (i *Ingester) InitFlushQueues() {
	i.flushQueuesDone.Add(i.cfg.ConcurrentFlushes)
	for j := 0; j < i.cfg.ConcurrentFlushes; j++ {
		i.flushQueues[j] = util.NewPriorityQueue(i.metrics.flushQueueLength)
		go i.flushLoop(j)
	}
}

// Flush implements ring.FlushTransferer
// Flush triggers a flush of all the chunks and closes the flush queues.
// Called from the Lifecycler as part of the ingester shutdown.
func (i *Ingester) Flush() {
	i.flush(true)
}

// TransferOut implements ring.FlushTransferer
// Noop implementation because ingesters have a WAL now that does not require transferring chunks any more.
// We return ErrTransferDisabled to indicate that we don't support transfers, and therefore we may flush on shutdown if configured to do so.
func (i *Ingester) TransferOut(_ context.Context) error {
	return ring.ErrTransferDisabled
}

func (i *Ingester) flush(mayRemoveStreams bool) {
	i.sweepUsers(true, mayRemoveStreams)

	// Close the flush queues, to unblock waiting workers.
	for _, flushQueue := range i.flushQueues {
		flushQueue.Close()
	}

	i.flushQueuesDone.Wait()
	level.Debug(i.logger).Log("msg", "flush queues have drained")
}

// FlushHandler triggers a flush of all in memory chunks.  Mainly used for
// local testing.
func (i *Ingester) FlushHandler(w http.ResponseWriter, _ *http.Request) {
	i.sweepUsers(true, true)
	w.WriteHeader(http.StatusNoContent)
}

type flushOp struct {
	from      model.Time
	userID    string
	fp        model.Fingerprint
	immediate bool
}

func (o *flushOp) Key() string {
	return fmt.Sprintf("%s-%s-%v", o.userID, o.fp, o.immediate)
}

func (o *flushOp) Priority() int64 {
	return -int64(o.from)
}

// sweepUsers periodically schedules series for flushing and garbage collects users with no streams
func (i *Ingester) sweepUsers(immediate, mayRemoveStreams bool) {
	instances := i.getInstances()

	for _, instance := range instances {
		i.sweepInstance(instance, immediate, mayRemoveStreams)
	}
	i.setFlushRate()
}

func (i *Ingester) sweepInstance(instance *instance, immediate, mayRemoveStreams bool) {
	_ = instance.streams.ForEach(func(s *stream) (bool, error) {
		i.sweepStream(instance, s, immediate)
		i.removeFlushedChunks(instance, s, mayRemoveStreams)
		return true, nil
	})
}

func (i *Ingester) sweepStream(instance *instance, stream *stream, immediate bool) {
	stream.chunkMtx.RLock()
	defer stream.chunkMtx.RUnlock()
	if len(stream.chunks) == 0 {
		return
	}

	lastChunk := stream.chunks[len(stream.chunks)-1]
	shouldFlush, _ := i.shouldFlushChunk(&lastChunk)
	if len(stream.chunks) == 1 && !immediate && !shouldFlush && !instance.ownedStreamsSvc.isStreamNotOwned(stream.fp) {
		return
	}

	flushQueueIndex := int(uint64(stream.fp) % uint64(i.cfg.ConcurrentFlushes))
	firstTime, _ := stream.chunks[0].chunk.Bounds()
	i.flushQueues[flushQueueIndex].Enqueue(&flushOp{
		model.TimeFromUnixNano(firstTime.UnixNano()), instance.instanceID,
		stream.fp, immediate,
	})
}

// Compute a rate such to spread calls to the store over nearly all of the flush period,
// for example if we have 600 items in the queue and period 1 min we will send 10.5 per second.
// Note if the store can't keep up with this rate then it doesn't make any difference.
func (i *Ingester) setFlushRate() {
	totalQueueLength := 0
	for _, q := range i.flushQueues {
		totalQueueLength += q.Length()
	}
	const jitter = 1.05 // aim to finish a little bit before the end of the period
	flushesPerSecond := float64(totalQueueLength) / i.cfg.FlushCheckPeriod.Seconds() * jitter
	// Avoid going very slowly with tiny queues
	if flushesPerSecond*i.cfg.FlushCheckPeriod.Seconds() < minFlushes {
		flushesPerSecond = minFlushes / i.cfg.FlushCheckPeriod.Seconds()
	}
	level.Debug(util_log.Logger).Log("msg", "computed flush rate", "rate", flushesPerSecond)
	i.flushRateLimiter.SetLimit(rate.Limit(flushesPerSecond))
}

func (i *Ingester) flushLoop(j int) {
	l := log.With(i.logger, "loop", j)
	defer func() {
		level.Debug(l).Log("msg", "Ingester.flushLoop() exited")
		i.flushQueuesDone.Done()
	}()

	for {
		o := i.flushQueues[j].Dequeue()
		if o == nil {
			return
		}
		op := o.(*flushOp)

		if !op.immediate {
			_ = i.flushRateLimiter.Wait(context.Background())
		}

		m := util_log.WithUserID(op.userID, l)
		err := i.flushOp(m, op)
		if err != nil {
			level.Error(m).Log("msg", "failed to flush", "err", err)
		}

		// If we're exiting & we failed to flush, put the failed operation
		// back in the queue at a later point.
		if op.immediate && err != nil {
			op.from = op.from.Add(flushBackoff)
			i.flushQueues[j].Enqueue(op)
		}
	}
}

func (i *Ingester) flushOp(l log.Logger, op *flushOp) error {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	b := backoff.New(ctx, i.cfg.FlushOpBackoff)
	for b.Ongoing() {
		err := i.flushUserSeries(ctx, op.userID, op.fp, op.immediate)
		if err == nil {
			break
		}
		level.Error(l).Log("msg", "failed to flush", "retries", b.NumRetries(), "err", err)
		b.Wait()
	}
	return b.Err()
}

func (i *Ingester) flushUserSeries(ctx context.Context, userID string, fp model.Fingerprint, immediate bool) error {
	instance, ok := i.getInstanceByID(userID)
	if !ok {
		return nil
	}

	chunks, labels, chunkMtx := i.collectChunksToFlush(instance, fp, immediate)
	if len(chunks) < 1 {
		return nil
	}

	totalCompressedSize := 0
	totalUncompressedSize := 0
	frc := flushReasonCounter{}
	for _, c := range chunks {
		totalCompressedSize += c.chunk.CompressedSize()
		totalUncompressedSize += c.chunk.UncompressedSize()
		err := frc.IncrementForReason(c.reason)
		if err != nil {
			level.Error(i.logger).Log("msg", "error incrementing flush reason", "err", err)
		}
	}

	lbs := labels.String()
	logValues := make([]interface{}, 0, 35)
	logValues = append(logValues,
		"msg", "flushing stream",
		"user", userID,
		"fp", fp,
		"immediate", immediate,
		"num_chunks", len(chunks),
		"total_comp", humanize.Bytes(uint64(totalCompressedSize)),
		"avg_comp", humanize.Bytes(uint64(totalCompressedSize/len(chunks))),
		"total_uncomp", humanize.Bytes(uint64(totalUncompressedSize)),
		"avg_uncomp", humanize.Bytes(uint64(totalUncompressedSize/len(chunks))))
	logValues = append(logValues, frc.Log()...)
	logValues = append(logValues, "labels", lbs)
	level.Info(i.logger).Log(logValues...)

	ctx = user.InjectOrgID(ctx, userID)
	ctx, cancelFunc := context.WithTimeout(ctx, i.cfg.FlushOpTimeout)
	defer cancelFunc()
	err := i.flushChunks(ctx, fp, labels, chunks, chunkMtx)
	if err != nil {
		return fmt.Errorf("failed to flush chunks: %w, num_chunks: %d, labels: %s", err, len(chunks), lbs)
	}

	return nil
}

func (i *Ingester) collectChunksToFlush(instance *instance, fp model.Fingerprint, immediate bool) ([]*chunkDesc, labels.Labels, *sync.RWMutex) {
	var stream *stream
	var ok bool
	stream, ok = instance.streams.LoadByFP(fp)

	if !ok {
		return nil, labels.EmptyLabels(), nil
	}

	stream.chunkMtx.Lock()
	defer stream.chunkMtx.Unlock()
	notOwnedStream := instance.ownedStreamsSvc.isStreamNotOwned(fp)

	var result []*chunkDesc
	for j := range stream.chunks {
		shouldFlush, reason := i.shouldFlushChunk(&stream.chunks[j])
		if !shouldFlush && notOwnedStream {
			shouldFlush, reason = true, flushReasonNotOwned
		}
		if immediate || shouldFlush {
			// Ensure no more writes happen to this chunk.
			if !stream.chunks[j].closed {
				stream.chunks[j].closed = true
			}
			// Flush this chunk if it hasn't already been successfully flushed.
			if stream.chunks[j].flushed.IsZero() {
				if immediate {
					reason = flushReasonForced
				}
				stream.chunks[j].reason = reason

				result = append(result, &stream.chunks[j])
			}
		}
	}
	return result, stream.labels, &stream.chunkMtx
}

func (i *Ingester) shouldFlushChunk(chunk *chunkDesc) (bool, string) {
	// Append should close the chunk when the a new one is added.
	if chunk.closed {
		if chunk.synced {
			return true, flushReasonSynced
		}
		return true, flushReasonFull
	}

	if time.Since(chunk.lastUpdated) > i.cfg.MaxChunkIdle {
		return true, flushReasonIdle
	}

	if from, to := chunk.chunk.Bounds(); to.Sub(from) > i.cfg.MaxChunkAge {
		return true, flushReasonMaxAge
	}

	return false, ""
}

func (i *Ingester) removeFlushedChunks(instance *instance, stream *stream, mayRemoveStream bool) {
	now := time.Now()

	stream.chunkMtx.Lock()
	defer stream.chunkMtx.Unlock()
	prevNumChunks := len(stream.chunks)
	var subtracted int
	for len(stream.chunks) > 0 {
		if stream.chunks[0].flushed.IsZero() || now.Sub(stream.chunks[0].flushed) < i.cfg.RetainPeriod {
			break
		}

		subtracted += stream.chunks[0].chunk.UncompressedSize()
		stream.chunks[0].chunk = nil // erase reference so the chunk can be garbage-collected
		stream.chunks = stream.chunks[1:]
	}
	i.metrics.memoryChunks.Sub(float64(prevNumChunks - len(stream.chunks)))

	// Signal how much data has been flushed to lessen any WAL replay pressure.
	i.replayController.Sub(int64(subtracted))

	if mayRemoveStream && len(stream.chunks) == 0 {
		// Unlock first, then lock inside streams' lock to prevent deadlock
		stream.chunkMtx.Unlock()
		// Only lock streamsMap when it's needed to remove a stream
		instance.streams.WithLock(func() {
			stream.chunkMtx.Lock()
			// Double check length
			if len(stream.chunks) == 0 {
				instance.removeStream(stream)
			}
		})
	}
}

// flushChunks iterates over given chunkDescs, derives chunk.Chunk from them and flush them to the store, one at a time.
//
// If a chunk fails to be flushed, this operation is reinserted in the queue. Since previously flushed chunks
// are marked as flushed, they shouldn't be flushed again.
// It has to close given chunks to have have the head block included.
func (i *Ingester) flushChunks(ctx context.Context, fp model.Fingerprint, labelPairs labels.Labels, cs []*chunkDesc, chunkMtx sync.Locker) error {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}

	// NB(owen-d): No longer needed in TSDB (and is removed in that code path)
	// It's required by historical index stores so we keep it for now.
	labelsBuilder := labels.NewBuilder(labelPairs)
	labelsBuilder.Set(nameLabel, logsValue)
	metric := labelsBuilder.Labels()

	sizePerTenant := i.metrics.chunkSizePerTenant.WithLabelValues(userID)
	countPerTenant := i.metrics.chunksPerTenant.WithLabelValues(userID)

	refs := make([]logproto.ChunkRef, 0, len(cs))
	for j, c := range cs {
		if err := i.closeChunk(c, chunkMtx); err != nil {
			return fmt.Errorf("chunk close for flushing: %w", err)
		}

		firstTime, lastTime := util.RoundToMilliseconds(c.chunk.Bounds())
		ch := chunk.NewChunk(
			userID, fp, metric,
			chunkenc.NewFacade(c.chunk, i.cfg.BlockSize, i.cfg.TargetChunkSize),
			firstTime,
			lastTime,
		)

		// encodeChunk mutates the chunk so we must pass by reference
		if err := i.encodeChunk(ctx, &ch, c); err != nil {
			return err
		}

		if err := i.flushChunk(ctx, &ch); err != nil {
			return err
		}

		refs = append(refs, ch.ChunkRef)

		reason := func() string {
			chunkMtx.Lock()
			defer chunkMtx.Unlock()

			return c.reason
		}()

		i.reportFlushedChunkStatistics(&ch, c, sizePerTenant, countPerTenant, reason)
		i.markChunkAsFlushed(cs[j], chunkMtx)
	}

	if _, ok := i.bbfConfig.EnabledTenantsMap[userID]; ok {
		background := user.InjectOrgID(context.Background(), userID)
		ctxWithTimeOut, _ := context.WithTimeout(background, 10*time.Minute)
		go i.writeToBloomWithRetries(ctxWithTimeOut, refs, labelPairs)
	}
	return nil
}

func (i *Ingester) writeToBloomWithRetries(ctx context.Context, refs []logproto.ChunkRef, labelPairs labels.Labels) {
	cfg := backoff.Config{
		MinBackoff: 2 * time.Second,
		MaxBackoff: 10 * time.Second,
		MaxRetries: 0,
	}
	retries := backoff.New(ctx, cfg)
	for retries.Ongoing() {

		err := i.writeChunkRefsToBloom(ctx, refs, labelPairs)
		if err == nil {
			return
		}

		level.Warn(util_log.Logger).Log("msg", "error writing tokens to BBF computer, will retry it", "err", err, "retry", retries.NumRetries())
		retries.Wait()
	}
	return
}

func (i *Ingester) writeChunkRefsToBloom(ctx context.Context, refs []logproto.ChunkRef, labelPairs labels.Labels) error {
	if len(refs) == 0 {
		return nil
	}

	s := config.SchemaConfig{
		Configs: i.periodicConfigs,
	}
	// build Request for each Computer
	rs, err := i.computerRing.Ring.GetAllHealthy(ring.Read)
	if err != nil {
		level.Error(i.logger).Log("msg", "failed to get all healthy bloom computers", "err", err)
		return err
	}
	if len(rs.Instances) == 0 {
		level.Error(i.logger).Log("msg", "no bloom computer registered")
		return errors.New("no bloom computer registered")
	}

	computers := rs.Instances
	slices.SortFunc(computers, func(i, j ring.InstanceDesc) int {
		return strings.Compare(i.Addr, j.Addr)
	})

	// 先对 refs 按 day time 分组一下
	group := make(map[config.DayTime][]*logproto.ChunkRef, 1)
	for _, ref := range refs {
		dayTime := config.NewDayTime(ref.Through)
		group[dayTime] = append(group[dayTime], &ref)
	}
	reqs := make(map[string]bbf.ComputeRequest, len(computers))
	for day, g := range group {
		schema, err := s.SchemaForTime(day.Time)
		if err != nil {
			level.Error(i.logger).Log("msg", "failed to get schema for time", "err", err)
			return err
		}
		if !MatchBloomStreams(schema, labelPairs) {
			continue
		}

		for _, ref := range g {
			id := s.ExternalKey(*ref)
			idx := encoding.XxhashHashIndex([]byte(id), len(computers))

			req, ok := reqs[computers[idx].Addr]
			if !ok {
				req = bbf.ComputeRequest{
					Refs: make([]*logproto.ChunkRef, 0, 10),
				}
				req.Refs = append(req.Refs, ref)
				reqs[computers[idx].Addr] = req
			}
			req.Refs = append(req.Refs, ref)
		}
	}

	level.Debug(i.logger).Log("msg", "write chunk refs to bloom", "reqs", reqs)
	// 写入 bloom computer
	for addr, req := range reqs {
		client, err := i.bbfComputerClient.GetClientFor(addr)
		if err != nil {
			level.Error(i.logger).Log("msg", "failed to get BBFComputerClient for addr", "err", err)
			return err
		}
		_, err = client.(bbf.BBFComputerClient).Compute(ctx, &req)
		if err != nil {
			level.Error(i.logger).Log("msg", "failed to write to bloom computer", "err", err)
			return err
		}
		level.Debug(i.logger).Log("msg", "success to write chunk refs to bloom computer", "addr", addr, "refs", len(req.Refs))
	}
	return nil
}

func MatchBloomStreams(schema config.PeriodConfig, labelPairs labels.Labels) bool {
	for _, _stream := range schema.BBFStreams {
		if labels.Selector(_stream.Matchers).Matches(labelPairs) {
			return true
		}
	}
	return false
}

// markChunkAsFlushed mark a chunk to make sure it won't be flushed if this operation fails.
func (i *Ingester) markChunkAsFlushed(desc *chunkDesc, chunkMtx sync.Locker) {
	chunkMtx.Lock()
	defer chunkMtx.Unlock()
	desc.flushed = time.Now()
}

// closeChunk closes the given chunk while locking it to ensure that new blocks are cut before flushing.
//
// If the chunk isn't closed, data in the head block isn't included.
func (i *Ingester) closeChunk(desc *chunkDesc, chunkMtx sync.Locker) error {
	chunkMtx.Lock()
	defer chunkMtx.Unlock()

	return desc.chunk.Close()
}

// encodeChunk encodes a chunk.Chunk based on the given chunkDesc.
//
// If the encoding is unsuccessful the flush operation is reinserted in the queue which will cause
// the encoding for a given chunk to be evaluated again.
func (i *Ingester) encodeChunk(ctx context.Context, ch *chunk.Chunk, desc *chunkDesc) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	start := time.Now()
	chunkBytesSize := desc.chunk.BytesSize() + 4*1024 // size + 4kB should be enough room for cortex header
	if err := ch.EncodeTo(bytes.NewBuffer(make([]byte, 0, chunkBytesSize)), i.logger); err != nil {
		if !errors.Is(err, chunk.ErrChunkDecode) {
			return fmt.Errorf("chunk encoding: %w", err)
		}

		i.metrics.chunkDecodeFailures.WithLabelValues(ch.UserID).Inc()
	}
	i.metrics.chunkEncodeTime.Observe(time.Since(start).Seconds())
	i.metrics.chunksEncoded.WithLabelValues(ch.UserID).Inc()
	return nil
}

// flushChunk flushes the given chunk to the store.
//
// If the flush is successful, metrics for this flush are to be reported.
// If the flush isn't successful, the operation for this userID is requeued allowing this and all other unflushed
// chunk to have another opportunity to be flushed.
func (i *Ingester) flushChunk(ctx context.Context, ch *chunk.Chunk) error {
	if err := i.store.Put(ctx, []chunk.Chunk{*ch}); err != nil {
		i.metrics.chunksFlushFailures.Inc()
		return fmt.Errorf("store put chunk: %w", err)
	}
	i.metrics.flushedChunksStats.Inc(1)
	return nil
}

// reportFlushedChunkStatistics calculate overall statistics of flushed chunks without compromising the flush process.
func (i *Ingester) reportFlushedChunkStatistics(ch *chunk.Chunk, desc *chunkDesc, sizePerTenant prometheus.Counter, countPerTenant prometheus.Counter, reason string) {
	byt, err := ch.Encoded()
	if err != nil {
		level.Error(i.logger).Log("msg", "failed to encode flushed wire chunk", "err", err)
		return
	}

	i.metrics.chunksFlushedPerReason.WithLabelValues(reason).Add(1)

	compressedSize := float64(len(byt))
	uncompressedSize, ok := chunkenc.UncompressedSize(ch.Data)

	if ok && compressedSize > 0 {
		i.metrics.chunkCompressionRatio.Observe(float64(uncompressedSize) / compressedSize)
	}

	utilization := ch.Data.Utilization()
	i.metrics.chunkUtilization.Observe(utilization)
	numEntries := desc.chunk.Size()
	i.metrics.chunkEntries.Observe(float64(numEntries))
	i.metrics.chunkSize.Observe(compressedSize)
	sizePerTenant.Add(compressedSize)
	countPerTenant.Inc()

	boundsFrom, boundsTo := desc.chunk.Bounds()
	i.metrics.chunkAge.Observe(time.Since(boundsFrom).Seconds())
	i.metrics.chunkLifespan.Observe(boundsTo.Sub(boundsFrom).Hours())

	i.metrics.flushedChunksBytesStats.Record(compressedSize)
	i.metrics.flushedChunksLinesStats.Record(float64(numEntries))
	i.metrics.flushedChunksUtilizationStats.Record(utilization)
	i.metrics.flushedChunksAgeStats.Record(time.Since(boundsFrom).Seconds())
	i.metrics.flushedChunksLifespanStats.Record(boundsTo.Sub(boundsFrom).Seconds())
}
