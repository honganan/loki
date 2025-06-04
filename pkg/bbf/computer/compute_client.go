package computer

import (
	"io"

	"github.com/grafana/loki/v3/pkg/bbf"
	"github.com/grafana/loki/v3/pkg/util/server"

	"github.com/grafana/dskit/middleware"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var computeClientRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "loki_bloom_compute_client_request_duration_seconds",
	Help:    "Time spent doing bloom Computer requests.",
	Buckets: prometheus.ExponentialBuckets(0.001, 4, 6),
}, []string{"operation", "status_code"})

type HealthAndComputerClient interface {
	grpc_health_v1.HealthClient
	Close() error
}

type ClosableHealthAndComputerClient struct {
	bbf.BBFComputerClient
	grpc_health_v1.HealthClient
	io.Closer
}

// New returns a new computer client.
func NewClient(cfg bbf.ClientConfig, addr string) (HealthAndComputerClient, error) {
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(cfg.GRPCClientConfig.CallOptions()...),
	}

	unaryInterceptors, streamInterceptors := instrumentation(&cfg)
	dialOpts, err := cfg.GRPCClientConfig.DialOption(unaryInterceptors, streamInterceptors, middleware.NoOpInvalidClusterValidationReporter)
	if err != nil {
		return nil, err
	}

	opts = append(opts, dialOpts...)
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	return ClosableHealthAndComputerClient{
		BBFComputerClient: bbf.NewBBFComputerClient(conn),
		HealthClient:      grpc_health_v1.NewHealthClient(conn),
		Closer:            conn,
	}, nil
}

func instrumentation(cfg *bbf.ClientConfig) ([]grpc.UnaryClientInterceptor, []grpc.StreamClientInterceptor) {
	var unaryInterceptors []grpc.UnaryClientInterceptor
	unaryInterceptors = append(unaryInterceptors, cfg.GRPCUnaryClientInterceptors...)
	unaryInterceptors = append(unaryInterceptors, server.UnaryClientQueryTagsInterceptor)
	unaryInterceptors = append(unaryInterceptors, otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()))
	if !cfg.Internal {
		unaryInterceptors = append(unaryInterceptors, middleware.ClientUserHeaderInterceptor)
	}
	unaryInterceptors = append(unaryInterceptors, middleware.UnaryClientInstrumentInterceptor(computeClientRequestDuration))

	var streamInterceptors []grpc.StreamClientInterceptor
	streamInterceptors = append(streamInterceptors, cfg.GRCPStreamClientInterceptors...)
	streamInterceptors = append(streamInterceptors, server.StreamClientQueryTagsInterceptor)
	streamInterceptors = append(streamInterceptors, otgrpc.OpenTracingStreamClientInterceptor(opentracing.GlobalTracer()))
	if !cfg.Internal {
		streamInterceptors = append(streamInterceptors, middleware.StreamClientUserHeaderInterceptor)
	}
	streamInterceptors = append(streamInterceptors, middleware.StreamClientInstrumentInterceptor(computeClientRequestDuration))

	return unaryInterceptors, streamInterceptors
}
