package computer

import (
	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/loki/v3/pkg/bbf"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var clients prometheus.Gauge

func NewComputerClientPool(name string, cfg bbf.PoolConfig, ring ring.ReadRing, factory bbf.PoolFactory, logger log.Logger, metricsNamespace string) *bbf.Pool {
	if clients == nil {
		clients = promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "compute_clients",
			Help:      "The current number of pattern computer clients.",
		})
	}
	pool := bbf.NewPool(name, cfg, bbf.NewRingServiceDiscovery(ring), factory, clients, logger)
	return pool
}
