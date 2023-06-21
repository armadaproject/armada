package reports

import (
	"context"

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
	return s.client.GetSchedulingReport(ctx, request)
}

func (s *ProxyingSchedulingReportsServer) GetQueueReport(ctx context.Context, request *schedulerobjects.QueueReportRequest) (*schedulerobjects.QueueReport, error) {
	return s.client.GetQueueReport(ctx, request)
}

func (s *ProxyingSchedulingReportsServer) GetJobReport(ctx context.Context, request *schedulerobjects.JobReportRequest) (*schedulerobjects.JobReport, error) {
	return s.client.GetJobReport(ctx, request)
}
