package bbf

import (
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/prometheus/common/model"
)

// buildChunkBloomFilterKey build bloom filter key in format of: ${tenant}_YYYYMMDDhhmm
// where the time bucket is calculated based on the through time of the chunk
func buildChunkBloomFilterKey(chk *logproto.ChunkRef) string {
	return bloomBucket(chk.Through.Time())
}

func bloomBucket(t time.Time) string {
	t1 := t.Unix() - (t.Unix() % (10 * int64(time.Minute.Seconds())))
	return time.Unix(t1, 0).UTC().Format("200601021504")
}

func bloomBucketTime(t model.Time) model.Time {
	t1 := t.Unix() - (t.Unix() % (10 * int64(time.Minute.Seconds())))
	return model.TimeFromUnix(t1)
}
