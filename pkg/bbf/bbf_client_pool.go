package bbf

import (
	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var clients prometheus.Gauge

func NewBBFClientPool(
	name string,
	cfg PoolConfig,
	ring ring.ReadRing,
	factory PoolFactory,
	logger log.Logger,
	metricsNamespace string,
) *Pool {

	if clients == nil {
		clients = promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "bbf_clients",
			Help:      "The current number of bbf clients.",
		})
	}
	return NewPool(name, cfg, NewRingServiceDiscovery(ring), factory, clients, logger)
}
