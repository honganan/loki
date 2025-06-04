package bbf

import (
	"flag"
	"time"

	"github.com/grafana/loki/v3/pkg/storage/chunk/cache"
	"github.com/grafana/loki/v3/pkg/util/ring"
)

type Config struct {
	QueryEnabled bool `yaml:"query_enabled" doc:"description=Enable query for bbf data."`
	//LifecyclerConfig ring.LifecyclerConfig `yaml:"lifecycler,omitempty" doc:"description=Configures how the lifecycle of the pattern ingester will operate and where it will register for discovery."`
	RingConfig   ring.RingConfig `yaml:"ring"`
	ClientConfig ClientConfig    `yaml:"client_config,omitempty" doc:"description=Configures how the pattern ingester will connect to the ingesters."`

	WalPath string `yaml:"wal_path"`
	Path    string `yaml:"path"`

	ComputeChunksBatch        int `yaml:"compute_chunks_batch"`
	ComputeParallelism        int `yaml:"compute_parallelism"`
	WriteParallelism          int `yaml:"write_parallelism"`
	WriteBatchSize            int `yaml:"write_batch_size"`
	FilterDownloadParallelism int `yaml:"filter_download_parallelism"`

	ShardCapacity int `yaml:"shard_capacity"`
	WindowSize    int `yaml:"window_size"`

	FlushInterval     time.Duration `yaml:"flush_interval"`
	IdleFlushDuration time.Duration `yaml:"idle_flush_duration"`
	GcInterval        time.Duration `yaml:"gc_interval"`
	Retention         time.Duration `yaml:"retention"`

	RefreshInterval time.Duration `yaml:"refresh_interval"`

	EnabledTenants    []string            `yaml:"enabled_tenants"`
	EnabledTenantsMap map[string]struct{} `yaml:"-"`
	CacheCfg          cache.Config        `yaml:"cache"`

	Factory PoolFactory `yaml:"-"`
}

func (c *Config) RegisterFlagsWithPrefix(f *flag.FlagSet) {
	c.ClientConfig.RegisterFlags(f)
	c.ClientConfig.Internal = false

	f.BoolVar(&c.QueryEnabled, "query-enabled", false, "Enable query for bbf data")
	f.StringVar(&c.Path, "path", "", "Path to store bbf data")
	f.IntVar(&c.ComputeChunksBatch, "compute-chunks-batch", 1000, "Compute chunks batch")
	f.IntVar(&c.ComputeParallelism, "compute-parallelism", 10, "Compute parallelism")
	f.IntVar(&c.WriteParallelism, "write-parallelism", 8, "Write parallelism")
	f.IntVar(&c.ShardCapacity, "shard-capacity", 2_000_000, "Shard capacity")
	f.IntVar(&c.WindowSize, "window-size", 6, "Window size")
	f.IntVar(&c.FilterDownloadParallelism, "filter-download-parallelism", 300, "Filter download parallelism")
	f.DurationVar(&c.FlushInterval, "flush-interval", time.Minute*5, "Interval to flush bbf data")
	f.DurationVar(&c.IdleFlushDuration, "idle-flush-duration", time.Minute*2, "Idle flush duration")
	f.DurationVar(&c.GcInterval, "gc-interval", time.Hour, "Interval to gc bbf data")
	f.DurationVar(&c.Retention, "retention", time.Hour*24*7, "Retention of bbf data")
	f.DurationVar(&c.RefreshInterval, "refresh-interval", time.Minute, "Interval to refresh bbf data")

	if len(c.EnabledTenants) == 0 {
		c.EnabledTenants = []string{"fake"}
	}
	c.EnabledTenantsMap = make(map[string]struct{}, len(c.EnabledTenants))
	for _, tenant := range c.EnabledTenants {
		c.EnabledTenantsMap[tenant] = struct{}{}
	}
	c.CacheCfg.RegisterFlagsWithPrefix("bbf.", "BBF cache config.", f)
}
