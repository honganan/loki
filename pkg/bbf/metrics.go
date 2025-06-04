package bbf

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	chunkSize prometheus.Histogram // uncompressed size of all chunks summed per series

	LoadFromDiskFiltersCounter *prometheus.CounterVec

	DiskReadElapsedHistogram       *prometheus.HistogramVec
	ReadFromCacheFiltersCounter    prometheus.Counter
	FetchFilterObjElapsedHistogram *prometheus.HistogramVec
	LoadFromS3FiltersCounter       *prometheus.CounterVec
	DownloadQueueEnqueueTime       prometheus.Histogram
}

func NewMetrics(r prometheus.Registerer) *Metrics {
	return &Metrics{
		chunkSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "loki",
			Subsystem: "bbf",
			Name:      "chunk_size",
			Help:      "uncompressed size of all chunks summed per series",
			Buckets:   prometheus.ExponentialBuckets(1, 10, 8),
		}),
		LoadFromDiskFiltersCounter: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "loki_bbf_load_from_disk_files_count",
			Help: "Filters load from disk counter, for read or write.",
		}, []string{"for"}),
		DiskReadElapsedHistogram: promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "loki_bbf_disk_read_elapsed_milliseconds",
			Help:    "Disk read elapsed milliseconds, for read or write.",
			Buckets: prometheus.ExponentialBuckets(4, 2, 8),
		}, []string{"for"}),
		FetchFilterObjElapsedHistogram: promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "loki_bbf_fetch_filter_obj_elapsed_milliseconds",
			Help:    "Fetch filter object elapsed milliseconds.",
			Buckets: prometheus.ExponentialBuckets(32, 2, 8),
		}, []string{"for"}),
		LoadFromS3FiltersCounter: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "loki_bbf_load_from_s3_files_count",
			Help: "Filters load from s3 counter, for read or write.",
		}, []string{"for"}),
		ReadFromCacheFiltersCounter: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "loki_bbf_read_from_cache_filters_count",
			Help: "Filters load from cache counter.",
		}),
		DownloadQueueEnqueueTime: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
			Name:    "loki_bbf_download_queue_enqueue_time_seconds",
			Buckets: prometheus.ExponentialBuckets(0.0001, 5, 8), // [0.0001, 0.0005, ... 7.8125]
			Help:    "Time in seconds it took to enqueue item to download queue",
		}),
	}
}
