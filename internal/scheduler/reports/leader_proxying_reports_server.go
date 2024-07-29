package reports

import (
	"context"
	"fmt"


	"google.golang.org/grpc"

	"github.com/armadaproject/armada/internal/common/armadacontext"

	"github.com/armadaproject/armada/internal/scheduler/leader"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

type LeaderProxyingSchedulingReportsServer struct {
	localReportsServer               schedulerobjects.SchedulerReportingServer
	leaderClientProvider             leader.LeaderClientConnectionProvider
	schedulerReportingClientProvider reportingClientProvider
}

func NewLeaderProxyingSchedulingReportsServer(
	schedulingReportsRepository schedulerobjects.SchedulerReportingServer,
	leaderClientProvider leader.LeaderClientConnectionProvider,
) *LeaderProxyingSchedulingReportsServer {
	return &LeaderProxyingSchedulingReportsServer{
		leaderClientProvider:             leaderClientProvider,
		localReportsServer:               schedulingReportsRepository,
		schedulerReportingClientProvider: &schedulerReportingClientProvider{},
	}
}

func (s *LeaderProxyingSchedulingReportsServer) GetSchedulingReport(ctx context.Context, request *schedulerobjects.SchedulingReportRequest) (*schedulerobjects.SchedulingReport, error) {
	isCurrentProcessLeader, leaderConnection, err := s.leaderClientProvider.GetCurrentLeaderClientConnection()
	if isCurrentProcessLeader {
		return s.localReportsServer.GetSchedulingReport(ctx, request)
	}
	if err != nil {
		return nil, err
	}
	leaderClient := s.schedulerReportingClientProvider.GetSchedulerReportingClient(leaderConnection)
	return leaderClient.GetSchedulingReport(ctx, request)
}

func (s *LeaderProxyingSchedulingReportsServer) GetQueueReport(ctx context.Context, request *schedulerobjects.QueueReportRequest) (*schedulerobjects.QueueReport, error) {
	isCurrentProcessLeader, leaderConnection, err := s.leaderClientProvider.GetCurrentLeaderClientConnection()
	if isCurrentProcessLeader {
		return s.localReportsServer.GetQueueReport(ctx, request)
	}
	if err != nil {
		return nil, err
	}
	leaderClient := s.schedulerReportingClientProvider.GetSchedulerReportingClient(leaderConnection)
	return leaderClient.GetQueueReport(ctx, request)
}

func (s *LeaderProxyingSchedulingReportsServer) GetJobReport(ctx context.Context, request *schedulerobjects.JobReportRequest) (*schedulerobjects.JobReport, error) {
	isCurrentProcessLeader, leaderConnection, err := s.leaderClientProvider.GetCurrentLeaderClientConnection()
	if isCurrentProcessLeader {
		return s.localReportsServer.GetJobReport(ctx, request)
	}
	if err != nil {
		return nil, err
	}
	leaderClient := s.schedulerReportingClientProvider.GetSchedulerReportingClient(leaderConnection)
	return leaderClient.GetJobReport(ctx, request)
}

func (s *LeaderProxyingSchedulingReportsServer) GetExecutors(req *schedulerobjects.StreamingExecutorGetRequest, srv schedulerobjects.SchedulerReporting_GetExecutorsServer) error {
	isCurrentProcessLeader, leaderConnection, err := s.leaderClientProvider.GetCurrentLeaderClientConnection()
	if isCurrentProcessLeader {
		return s.localReportsServer.GetExecutors(req, srv)
	}
	if err != nil {
		return err
	}
	ctx := armadacontext.FromGrpcCtx(srv.Context())
	leaderClient := s.schedulerReportingClientProvider.GetSchedulerReportingClient(leaderConnection)
	executorClient, err := leaderClient.GetExecutors(ctx, &schedulerobjects.StreamingExecutorGetRequest{
		Num: 0,
	})
	defer executorClient.CloseSend()
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("GetExecutors call interrupted")
		default:
			msg, err := executorClient.Recv()
			if err != nil {
				return err
			}

			err = srv.Send(msg)
			if err != nil {
				return err
			}

			switch msg.Event.(type) {
			case *schedulerobjects.StreamingExecutorMessage_End:
				return nil
			default:
			}
		}
	}
}

type reportingClientProvider interface {
	GetSchedulerReportingClient(conn *grpc.ClientConn) schedulerobjects.SchedulerReportingClient
}

type schedulerReportingClientProvider struct{}

func (s *schedulerReportingClientProvider) GetSchedulerReportingClient(conn *grpc.ClientConn) schedulerobjects.SchedulerReportingClient {
	return schedulerobjects.NewSchedulerReportingClient(conn)
}
