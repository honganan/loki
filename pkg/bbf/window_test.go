package bbf

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/consul"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newClient(reg prometheus.Registerer) (*consul.Client, error) {
	cfg := consul.Config{
		Host:              "localhost:8500",
		ACLToken:          flagext.SecretWithValue("4f9b7fec-ae6d-9923-cf12-a7fe9cd0ba47"),
		HTTPClientTimeout: 20 * time.Second,
		ConsistentReads:   true,
		WatchKeyRateLimit: 1,
		WatchKeyBurstSize: 1,
		CasRetryDelay:     1 * time.Second,
	}
	return consul.NewClient(cfg, codec.String{}, util_log.Logger, reg)
}

func TestWindow_Equal(t *testing.T) {
	reg := prometheus.NewRegistry()
	client, err := newClient(reg)
	require.NoError(t, err)

	ctx := context.Background()
	window := NewWindow(ctx, "host_test", 1, client, reg)

	defer func() {
		err = recursiveDelete(window, ctx, "window/")
		require.NoError(t, err)
		err = recursiveDelete(window, ctx, "candidate/")
		require.NoError(t, err)
	}()

	now := time.Now()
	bucket1 := model.TimeFromUnix(now.Unix())
	bucket2 := model.TimeFromUnix(now.Unix())
	assert.True(t, bucket1.Equal(bucket2))
	res, err := window.Expand(bloomBucket(bucket1.Time()))
	require.NoError(t, err)
	assert.True(t, res)
	res, err = window.Expand(bloomBucket(bucket2.Time()))
	require.NoError(t, err)
	assert.True(t, res)

	err = window.Shrink(bloomBucket(bucket1.Time()))
	require.NoError(t, err)
	err = window.Shrink(bloomBucket(bucket2.Time()))
	require.NoError(t, err)
}

func TestWindow_Expand(t *testing.T) {
	reg := prometheus.NewRegistry()
	client, err := newClient(reg)
	require.NoError(t, err)

	ctx := context.Background()
	window := NewWindow(ctx, "host_test", 3, client, reg)
	defer func() {
		err = recursiveDelete(window, ctx, "window/")
		require.NoError(t, err)
		err = recursiveDelete(window, ctx, "candidate/")
		require.NoError(t, err)
	}()

	now := time.Now()
	// Test expanding the window
	bucket1 := bloomBucket(now)
	res, err := window.Expand(bucket1)
	require.NoError(t, err)
	assert.True(t, res)
	assert.Equal(t, 1, window.Size())

	bucket2 := bloomBucket(now.Add(time.Hour))
	res, err = window.Expand(bucket2)
	require.NoError(t, err)
	assert.True(t, res)
	assert.Equal(t, 2, window.Size())

	bucket3 := bloomBucket(now.Add(2 * time.Hour))
	res, err = window.Expand(bucket3)
	require.NoError(t, err)
	assert.True(t, res)
	assert.Equal(t, 3, window.Size())

	// Test expanding beyond maxSize
	bucket4 := bloomBucket(now.Add(3 * time.Hour))
	res, err = window.Expand(bucket4)
	require.NoError(t, err)
	assert.False(t, res)
	assert.Equal(t, 3, window.Size())
	assert.Equal(t, 1, window.CandidateSize())

	// Verify metrics
	assert.Equal(t, 2, testutil.CollectAndCount(window.WindowSizeGauge))
}

func TestWindow_Shrink2(t *testing.T) {
	reg := prometheus.NewRegistry()
	client, err := newClient(reg)
	require.NoError(t, err)

	ctx := context.Background()
	window := NewWindow(ctx, "host_test", 6, client, reg)
	defer func() {
		err = recursiveDelete(window, ctx, "window/")
		require.NoError(t, err)
		err = recursiveDelete(window, ctx, "candidate/")
		require.NoError(t, err)
	}()

	window.Expand(bloomBucket(time.Unix(1726718400, 0)))
	window.Expand(bloomBucket(time.Unix(1726720200, 0)))
	window.Expand(bloomBucket(time.Unix(1726722000, 0)))
	window.Expand(bloomBucket(time.Unix(1726723200, 0)))
	window.Expand(bloomBucket(time.Unix(1726724400, 0)))
	window.Expand(bloomBucket(time.Unix(1726725600, 0)))

	window.Shrink(bloomBucket(time.Unix(1726722000, 0)))
	assert.Equal(t, 5, window.Size())
}

func TestWindow_Shrink(t *testing.T) {
	reg := prometheus.NewRegistry()
	client, err := newClient(reg)
	require.NoError(t, err)

	ctx := context.Background()
	window := NewWindow(ctx, "host_test", 3, client, reg)
	err = recursiveDelete(window, ctx, "window/")
	require.NoError(t, err)
	err = recursiveDelete(window, ctx, "candidate/")
	require.NoError(t, err)
	defer func() {
		err = recursiveDelete(window, ctx, "window/")
		require.NoError(t, err)
		err = recursiveDelete(window, ctx, "candidate/")
		require.NoError(t, err)
	}()

	now := time.Now()
	// Add elements to the window
	bucket1 := bloomBucket(now)
	bucket2 := bloomBucket(now.Add(time.Hour))
	bucket3 := bloomBucket(now.Add(2 * time.Hour))
	window.Expand(bucket1)
	window.Expand(bucket2)
	window.Expand(bucket3)

	// Add candidate element
	bucket4 := bloomBucket(now.Add(3 * time.Hour))
	res, err := window.Expand(bucket4)
	require.NoError(t, err)
	assert.False(t, res)
	assert.Equal(t, 1, window.CandidateSize())

	// Test shrinking the window
	window.Shrink(bucket2)
	assert.Equal(t, 3, window.Size())
	assert.True(t, strings.Contains(window.Elements()[0], bucket1))
	assert.True(t, strings.Contains(window.Elements()[1], bucket3))
	assert.True(t, strings.Contains(window.Elements()[2], bucket4))
	assert.Equal(t, 0, window.CandidateSize())

	// Verify metrics
	assert.Equal(t, 2, testutil.CollectAndCount(window.WindowSizeGauge))
}

func TestWindow_Candidate(t *testing.T) {
	reg := prometheus.NewRegistry()
	client, err := newClient(reg)
	require.NoError(t, err)

	ctx := context.Background()
	window := NewWindow(ctx, "host_test", 3, client, reg)

	defer func() {
		err = recursiveDelete(window, ctx, "window/")
		require.NoError(t, err)
		err = recursiveDelete(window, ctx, "candidate/")
		require.NoError(t, err)
	}()

	now := time.Now()
	// Add elements to the window
	bucket0 := bloomBucket(now)
	bucket1 := bloomBucket(now.Add(time.Hour))
	bucket4 := bloomBucket(now.Add(4 * time.Hour))
	res, err := window.Expand(bucket0)
	require.NoError(t, err)
	require.True(t, res)

	res, err = window.Expand(bucket1)
	require.NoError(t, err)
	require.True(t, res)

	res, err = window.Expand(bucket4)
	require.NoError(t, err)
	require.True(t, res)

	// Add candidate element
	bucket3 := bloomBucket(now.Add(3 * time.Hour))
	bucket5 := bloomBucket(now.Add(5 * time.Hour))
	res, err = window.Expand(bucket3)
	require.NoError(t, err)
	require.False(t, res)
	res, err = window.Expand(bucket5)
	require.NoError(t, err)
	require.False(t, res)

	// Test shrinking the window
	window.Shrink(bucket1)
	res, err = window.Expand(bucket5)
	require.NoError(t, err)
	require.False(t, res)

	assert.Equal(t, 3, window.Size())
	assert.True(t, strings.Contains(window.Elements()[0], bucket0))
	assert.True(t, strings.Contains(window.Elements()[1], bucket3))
	assert.True(t, strings.Contains(window.Elements()[2], bucket4))
	assert.True(t, strings.Contains(window.CandidateElements()[0], bucket5))
	assert.Equal(t, 1, window.CandidateSize())

	// Verify metrics
	assert.Equal(t, 2, testutil.CollectAndCount(window.WindowSizeGauge))
}

func TestWindow_Size(t *testing.T) {
	reg0 := prometheus.NewRegistry()
	reg1 := prometheus.NewRegistry()
	client, err := newClient(reg0)
	client1, err := newClient(reg1)
	require.NoError(t, err)

	ctx := context.Background()
	window0 := NewWindow(ctx, "host_test0", 3, client, reg0)
	window1 := NewWindow(ctx, "host_test1", 3, client1, reg1)

	defer func() {
		err = recursiveDelete(window0, ctx, "window/")
		require.NoError(t, err)
		err = recursiveDelete(window0, ctx, "candidate/")
		require.NoError(t, err)
	}()

	now := time.Now()
	bucket1 := bloomBucket(now)
	window0.Expand(bucket1)
	window1.Expand(bucket1)

	assert.Equal(t, 1, window0.Size())
	assert.Equal(t, 1, window1.Size())
}

func recursiveDelete(window *Window, ctx context.Context, key string) error {
	keys, err := window.kvClient.List(ctx, key)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := window.kvClient.Delete(ctx, key); err != nil {
			window.kvClient.Delete(ctx, key)
		}
	}
	return nil
}
