package scheduler

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

type LeaderProxyingSchedulingReportsServer struct {
	localReportsServer               schedulerobjects.SchedulerReportingServer
	leaderClientProvider             LeaderClientConnectionProvider
	leaderController                 LeaderController
	schedulerReportingClientProvider reportingClientProvider
}

func NewLeaderProxyingSchedulingReportsServer(
	schedulingReportsRepository schedulerobjects.SchedulerReportingServer,
	leaderController LeaderController,
	leaderClientProvider LeaderClientConnectionProvider,
) *LeaderProxyingSchedulingReportsServer {
	return &LeaderProxyingSchedulingReportsServer{
		leaderClientProvider:             leaderClientProvider,
		localReportsServer:               schedulingReportsRepository,
		leaderController:                 leaderController,
		schedulerReportingClientProvider: &schedulerReportingClientProvider{},
	}
}

func (s *LeaderProxyingSchedulingReportsServer) GetSchedulingReport(ctx context.Context, request *schedulerobjects.SchedulingReportRequest) (*schedulerobjects.SchedulingReport, error) {
	if s.isCurrentProcessLeader() {
		return s.localReportsServer.GetSchedulingReport(ctx, request)
	}
	leaderClient, err := s.getLeaderClient()
	if err != nil {
		return nil, err
	}
	return leaderClient.GetSchedulingReport(ctx, request)
}

func (s *LeaderProxyingSchedulingReportsServer) GetQueueReport(ctx context.Context, request *schedulerobjects.QueueReportRequest) (*schedulerobjects.QueueReport, error) {
	if s.isCurrentProcessLeader() {
		return s.localReportsServer.GetQueueReport(ctx, request)
	}
	leaderClient, err := s.getLeaderClient()
	if err != nil {
		return nil, err
	}
	return leaderClient.GetQueueReport(ctx, request)
}

func (s *LeaderProxyingSchedulingReportsServer) GetJobReport(ctx context.Context, request *schedulerobjects.JobReportRequest) (*schedulerobjects.JobReport, error) {
	if s.isCurrentProcessLeader() {
		return s.localReportsServer.GetJobReport(ctx, request)
	}
	leaderClient, err := s.getLeaderClient()
	if err != nil {
		return nil, err
	}
	return leaderClient.GetJobReport(ctx, request)
}

func (s *LeaderProxyingSchedulingReportsServer) getLeaderClient() (schedulerobjects.SchedulerReportingClient, error) {
	leaderConnection, err := s.leaderClientProvider.GetCurrentLeaderClientConnection()
	if err != nil {
		return nil, err
	}
	if leaderConnection == nil {
		return nil, fmt.Errorf("no client found for leader, unable to retrieve reports")
	}
	return s.schedulerReportingClientProvider.GetSchedulerReportingClient(leaderConnection), nil
}

func (s *LeaderProxyingSchedulingReportsServer) isCurrentProcessLeader() bool {
	return s.leaderController.ValidateToken(s.leaderController.GetToken())
}

type reportingClientProvider interface {
	GetSchedulerReportingClient(conn *grpc.ClientConn) schedulerobjects.SchedulerReportingClient
}

type schedulerReportingClientProvider struct{}

func (s *schedulerReportingClientProvider) GetSchedulerReportingClient(conn *grpc.ClientConn) schedulerobjects.SchedulerReportingClient {
	return schedulerobjects.NewSchedulerReportingClient(conn)
}
