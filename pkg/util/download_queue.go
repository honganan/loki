package util

import (
	"context"
	"sync"

	"github.com/dolthub/swiss"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"k8s.io/utils/keymutex"
)

type processFunc[T any, R any] func(context.Context, DownloadRequest[T, R])

type DownloadRequest[T any, R any] struct {
	Ctx     context.Context
	Item    T
	Key     string
	Idx     int
	Async   bool
	Results chan<- DownloadResponse[R]
	Errors  chan<- error
}

type DownloadResponse[R any] struct {
	Item R
	Key  string
	Idx  int
}

// DownloadQueue 是一个带下载队列和并发控制的工具，需要注意的是，在同步模式下，如果队列中加入了重复元素
// 将会被重复处理，如果需要去重，可以在调用 Enqueue 之前和在 processFunc 中分别进行缓存 double check
type DownloadQueue[T any, R any] struct {
	queue         chan DownloadRequest[T, R]
	enqueued      *swiss.Map[string, struct{}]
	enqueuedMutex sync.Mutex
	mu            keymutex.KeyMutex
	wg            sync.WaitGroup
	done          chan struct{}
	Process       processFunc[T, R]
	logger        log.Logger
}

func NewDownloadQueue[T any, R any](size, workers int, process processFunc[T, R], logger log.Logger) (*DownloadQueue[T, R], error) {
	if size < 1 {
		return nil, errors.New("queue size needs to be greater than 0")
	}
	if workers < 1 {
		return nil, errors.New("queue requires at least 1 worker")
	}
	q := &DownloadQueue[T, R]{
		queue:    make(chan DownloadRequest[T, R], size),
		enqueued: swiss.NewMap[string, struct{}](uint32(size)),
		mu:       keymutex.NewHashed(workers),
		done:     make(chan struct{}),
		Process:  process,
		logger:   logger,
	}
	for i := 0; i < workers; i++ {
		q.wg.Add(1)
		go q.runWorker()
	}
	return q, nil
}

func (q *DownloadQueue[T, R]) Enqueue(t DownloadRequest[T, R]) {
	if !t.Async {
		q.queue <- t
		return
	}
	// for async task we attempt to dedupe task already in progress.
	q.enqueuedMutex.Lock()
	defer q.enqueuedMutex.Unlock()
	if q.enqueued.Has(t.Key) {
		return
	}
	select {
	case q.queue <- t:
		q.enqueued.Put(t.Key, struct{}{})
	default:
		// todo we probably want a metric on dropped items
		level.Warn(q.logger).Log("msg", "download queue is full, dropping item", "key", t.Key)
		return
	}
}

func (q *DownloadQueue[T, R]) runWorker() {
	defer q.wg.Done()
	for {
		select {
		case <-q.done:
			return
		case task := <-q.queue:
			q.do(task.Ctx, task)
		}
	}
}

func (q *DownloadQueue[T, R]) do(ctx context.Context, task DownloadRequest[T, R]) {
	if ctx.Err() != nil {
		task.Errors <- ctx.Err()
		return
	}
	q.mu.LockKey(task.Key)
	defer func() {
		err := q.mu.UnlockKey(task.Key)
		if err != nil {
			level.Error(q.logger).Log("msg", "failed to unlock key in block lock", "key", task.Key, "err", err)
		}
		if task.Async {
			q.enqueuedMutex.Lock()
			_ = q.enqueued.Delete(task.Key)
			q.enqueuedMutex.Unlock()
		}
	}()

	q.Process(ctx, task)
}

func (q *DownloadQueue[T, R]) close() {
	close(q.done)
	q.wg.Wait()
}
