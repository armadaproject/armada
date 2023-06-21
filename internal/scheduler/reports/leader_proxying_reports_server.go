package reports

import (
	"context"

	"github.com/armadaproject/armada/internal/scheduler"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

type LeaderProxyingSchedulingReportsServer struct {
	localReportsServer   schedulerobjects.SchedulerReportingServer
	leaderClientProvider LeaderSchedulingReportClientProvider
	leaderController     scheduler.LeaderController
}

func NewLeaderProxyingSchedulingReportsServer(schedulingReportsRepository *scheduler.SchedulingContextRepository, leaderController scheduler.LeaderController) *LeaderProxyingSchedulingReportsServer {
	return &LeaderProxyingSchedulingReportsServer{
		localReportsServer: schedulingReportsRepository,
		leaderController:   leaderController,
	}
}

func (s *LeaderProxyingSchedulingReportsServer) GetSchedulingReport(ctx context.Context, request *schedulerobjects.SchedulingReportRequest) (*schedulerobjects.SchedulingReport, error) {
	if s.isCurrentProcessLeader() {
		return s.localReportsServer.GetSchedulingReport(ctx, request)
	}
	return s.getLeaderClient().GetSchedulingReport(ctx, request)
}

func (s *LeaderProxyingSchedulingReportsServer) GetQueueReport(ctx context.Context, request *schedulerobjects.QueueReportRequest) (*schedulerobjects.QueueReport, error) {
	if s.isCurrentProcessLeader() {
		return s.localReportsServer.GetQueueReport(ctx, request)
	}
	return s.getLeaderClient().GetQueueReport(ctx, request)
}

func (s *LeaderProxyingSchedulingReportsServer) GetJobReport(ctx context.Context, request *schedulerobjects.JobReportRequest) (*schedulerobjects.JobReport, error) {
	if s.isCurrentProcessLeader() {
		return s.localReportsServer.GetJobReport(ctx, request)
	}
	return s.getLeaderClient().GetJobReport(ctx, request)
}

func (s *LeaderProxyingSchedulingReportsServer) getLeaderClient() schedulerobjects.SchedulerReportingClient {
	return s.leaderClientProvider.GetCurrentLeaderClient()
}

func (s *LeaderProxyingSchedulingReportsServer) isCurrentProcessLeader() bool {
	return s.leaderController.ValidateToken(s.leaderController.GetToken())
}
