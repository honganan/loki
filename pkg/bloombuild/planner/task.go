package planner

import (
	"context"
	"time"

	"go.uber.org/atomic"

	"github.com/grafana/loki/v3/pkg/bloombuild/protos"
)

type QueueTask struct {
	*protos.Task

	resultsChannel chan *protos.TaskResult

	// Tracking
	timesEnqueued atomic.Int64
	queueTime     time.Time
	ctx           context.Context
}

func NewQueueTask(
	ctx context.Context,
	queueTime time.Time,
	task *protos.Task,
	resultsChannel chan *protos.TaskResult,
) *QueueTask {
	return &QueueTask{
		Task:           task,
		resultsChannel: resultsChannel,
		ctx:            ctx,
		queueTime:      queueTime,
	}
}

func (t QueueTask) Tenant() string {
	return t.Task.Tenant
}

func (t QueueTask) Table() string {
	return t.Task.Table.String()
}

func (t QueueTask) ID() string {
	return t.Task.ID
}
