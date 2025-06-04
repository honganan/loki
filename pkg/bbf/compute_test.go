package bbf

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var UnreadOffsetGauge = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Name: "loki_bbf_unread_offset",
		Help: "The unread offset of the BBF computer WAL.",
	})

func init() {
	prometheus.MustRegister(UnreadOffsetGauge)
}

func TestUnreadOffset(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}

	wal, err := NewWAL(ctx, "./test_loki", 5*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		wal.monitorUnreadOffset(UnreadOffsetGauge)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err = wal.Read(1000, 1024, func(ctx context.Context, chunks []string) {})
		if err != nil {
			t.Fatal(err)
			return
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		data := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(10 * time.Microsecond)
				err = wal.Write([]string{strconv.Itoa(data)})
				if err != nil {
					t.Fatal(err)
				}
				data++
			}
		}
	}()

	go func() {
		// 暴露指标接口
		http.Handle("/metrics", promhttp.Handler())
		t.Logf("server start at :8080 ...")
		http.ListenAndServe(":8080", nil)
	}()

	<-time.NewTicker(time.Minute).C
	cancelFunc()
	wg.Wait()

	glob, _ := filepath.Glob("test_loki.*")
	for _, file := range glob {
		os.Remove(file)
	}
}
