package scheduler

import (
	"context"
	"time"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

type ProxyingSchedulingReportsServer struct {
	client schedulerobjects.SchedulerReportingClient
}

func NewProxyingSchedulingReportsServer(client schedulerobjects.SchedulerReportingClient) *ProxyingSchedulingReportsServer {
	return &ProxyingSchedulingReportsServer{
		client: client,
	}
}

func (s *ProxyingSchedulingReportsServer) GetSchedulingReport(ctx context.Context, request *schedulerobjects.SchedulingReportRequest) (*schedulerobjects.SchedulingReport, error) {
	ctx, cancel := reduceTimeout(ctx)
	defer cancel()
	return s.client.GetSchedulingReport(ctx, request)
}

func (s *ProxyingSchedulingReportsServer) GetQueueReport(ctx context.Context, request *schedulerobjects.QueueReportRequest) (*schedulerobjects.QueueReport, error) {
	ctx, cancel := reduceTimeout(ctx)
	defer cancel()
	return s.client.GetQueueReport(ctx, request)
}

func (s *ProxyingSchedulingReportsServer) GetJobReport(ctx context.Context, request *schedulerobjects.JobReportRequest) (*schedulerobjects.JobReport, error) {
	ctx, cancel := reduceTimeout(ctx)
	defer cancel()
	return s.client.GetJobReport(ctx, request)
}

func reduceTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.WithCancel(ctx)
	}
	deadline = deadline.Add(-time.Second)
	return context.WithDeadline(ctx, deadline)
}
