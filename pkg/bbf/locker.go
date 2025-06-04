package bbf

import (
	"sync"
	"sync/atomic"
)

var mutexPool sync.Pool

type lockWrapper struct {
	ref int32
	sync.Mutex
}

func getMutex() *lockWrapper {
	v := mutexPool.Get()
	if v == nil {
		return &lockWrapper{}
	}
	return v.(*lockWrapper)
}

func putMutex(m *lockWrapper) {
	mutexPool.Put(m)
}

type locker[T comparable] struct {
	mu    sync.Mutex
	locks map[T]*lockWrapper
}

func newLocker[T comparable]() *locker[T] {
	return &locker[T]{
		locks: make(map[T]*lockWrapper),
	}
}

func (l *locker[T]) Lock(key T) {
	l.mu.Lock()
	mu, ok := l.locks[key]
	if !ok {
		mu = getMutex()
		l.locks[key] = mu
	}
	mu.ref++
	l.mu.Unlock()

	atomic.AddInt32(&mu.ref, 1)
	mu.Lock()
}

func (l *locker[T]) Unlock(key T) {
	l.mu.Lock()
	mu, ok := l.locks[key]
	if !ok {
		panic("unlock a not locked key")
	}

	mu.ref--
	if mu.ref == 0 {
		delete(l.locks, key)
	}

	l.mu.Unlock()

	mu.Unlock()
	if mu.ref == 0 {
		putMutex(mu)
	}
}
