package bbf

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/grafana/dskit/kv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/net/context"
)

type Window struct {
	ctx     context.Context
	maxSize int

	hostname        string
	kvClient        kv.Client
	mu              sync.Mutex
	WindowSizeGauge *prometheus.GaugeVec
}

func NewWindow(ctx context.Context, hostname string, maxSize int, kvClient kv.Client, reg prometheus.Registerer) *Window {

	return &Window{
		ctx:      ctx,
		maxSize:  maxSize,
		hostname: hostname,
		kvClient: kvClient,

		WindowSizeGauge: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "loki_bbf_window_size",
			Help: "The size of the window",
		}, []string{"for"}),
	}
}

// Expand tries to add a bucket to the window. If successful, returns true; otherwise, returns false.
func (w *Window) Expand(bucket string) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.expandWithoutLock(bucket)
}

// expandWithoutLock 尝试将 bucket 加入窗口（不使用锁）
func (w *Window) expandWithoutLock(bucket string) (bool, error) {
	windowKey := "window/" + bucket
	bucketKey := windowKey + "/" + w.hostname
	candidateKey := "candidate/" + bucket

	// Check if bucket is already in the window
	bucketKeys, err := w.kvClient.List(w.ctx, windowKey+"/")
	if err != nil {
		return false, err
	}

	if len(bucketKeys) > 0 {
		// Bucket already exists, add hostname with current time to the bucket
		err = w.kvClient.CAS(w.ctx, bucketKey, func(in interface{}) (out interface{}, retry bool, err error) {
			if in == nil {
				return time.Now().String(), false, nil
			}
			return in, false, nil
		})

		if err != nil {
			return false, err
		}

		w.WindowSizeGauge.WithLabelValues("elements").Set(float64(w.Size()))
		return true, nil
	}

	// Check current window size
	if w.Size() >= w.maxSize {
		// Add to candidate elements
		_ = w.kvClient.CAS(w.ctx, candidateKey, func(in interface{}) (out interface{}, retry bool, err error) {
			if in == nil {
				return "pending", false, nil
			}
			return in, false, nil
		})
		w.WindowSizeGauge.WithLabelValues("candidate").Set(float64(w.CandidateSize()))
		return false, nil
	}

	// Create the bucket directory and add hostname with current time to the bucket
	err = w.kvClient.CAS(w.ctx, bucketKey, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			return time.Now().String(), false, nil
		}
		return in, false, nil
	})

	if err != nil {
		return false, err
	}

	w.WindowSizeGauge.WithLabelValues("elements").Set(float64(w.Size()))
	return true, nil
}

// Shrink shrinks the window and tries to add the smallest task from the candidate queue to the window
func (w *Window) Shrink(bucket string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	windowKey := "window/" + bucket
	bucketKey := windowKey + "/" + w.hostname

	// Delete hostname from the bucket
	err := w.kvClient.Delete(w.ctx, bucketKey)
	if err != nil {
		return err
	}

	// Check if all nodes have completed the task for the given time
	allCompleted := true
	bucketKeys, err := w.kvClient.List(w.ctx, windowKey+"/")
	if err != nil {
		return err
	}

	if len(bucketKeys) > 0 {
		allCompleted = false
	}

	if allCompleted {
		err := w.kvClient.Delete(w.ctx, windowKey)
		if err != nil {
			return err
		}

		// Try to add the earliest pending task to the window using CAS for atomicity
		err = w.kvClient.CAS(w.ctx, "candidate/", func(in interface{}) (out interface{}, retry bool, err error) {
			candidateKeys, err := w.kvClient.List(w.ctx, "candidate/")
			if err != nil {
				return nil, true, err
			}

			if len(candidateKeys) == 0 {
				return nil, false, nil
			}

			sort.Strings(candidateKeys)
			earliestKey := candidateKeys[0]
			earliestBucket := earliestKey[len("candidate/"):]

			success, err := w.expandWithoutLock(earliestBucket)
			if err != nil {
				return nil, true, err
			}
			if success {
				_ = w.kvClient.Delete(w.ctx, earliestKey)
			}
			return in, false, nil
		})
		if err != nil {
			return err
		}
	}

	w.WindowSizeGauge.WithLabelValues("elements").Set(float64(w.Size()))
	w.WindowSizeGauge.WithLabelValues("candidate").Set(float64(w.CandidateSize()))
	return nil
}

func (w *Window) Elements() []string {
	windowKeys, err := w.kvClient.List(w.ctx, "window/")
	if err != nil {
		return nil
	}
	return windowKeys
}

func (w *Window) CandidateElements() []string {
	candidateKeys, err := w.kvClient.List(w.ctx, "candidate/")
	if err != nil {
		return nil
	}
	return candidateKeys
}

func (w *Window) Size() int {
	windowKeys, err := w.kvClient.List(w.ctx, "window/")
	if err != nil {
		return 0
	}
	m := make(map[string]struct{})
	for _, key := range windowKeys {
		key = key[len("window/"):]
		key = key[:strings.Index(key, "/")]
		m[key] = struct{}{}
	}
	return len(m)
}

func (w *Window) CandidateSize() int {
	candidateKeys, err := w.kvClient.List(w.ctx, "candidate/")
	if err != nil {
		return 0
	}
	return len(candidateKeys)
}

func (w *Window) String() string {
	var res string
	// 获取当前窗口中的所有元素
	windowKeys, err := w.kvClient.List(w.ctx, "window/")
	if err != nil {
		return "Error retrieving window elements"
	}
	res += "Window Elements:\n"
	for _, key := range windowKeys {
		res += key + "\n"
	}

	// 获取候选元素
	candidateKeys, err := w.kvClient.List(w.ctx, "candidate/")
	if err != nil {
		return "Error retrieving candidate elements"
	}
	res += "Candidate Elements:\n"
	for _, key := range candidateKeys {
		res += key + "\n"
	}
	return res
}
