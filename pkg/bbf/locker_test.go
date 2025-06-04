package bbf

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestLockAndUnlock(t *testing.T) {
	l := newLocker[string]()

	var count int64
	for i := 0; i < 2000; i++ {
		go func() {
			atomic.AddInt64(&count, 1)
			l.Lock("test")
			l.Unlock("test")
			atomic.AddInt64(&count, -1)
		}()
	}

	<-time.After(500 * time.Millisecond)
}

func BenchmarkLocker(b *testing.B) {
	l := newLocker[string]()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Lock("test")
		l.Unlock("test")
	}
}
