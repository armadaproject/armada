package reports

import (
	"context"
	"fmt"
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

func (s *ProxyingSchedulingReportsServer) GetExecutors(req *schedulerobjects.StreamingExecutorGetRequest, srv schedulerobjects.SchedulerReporting_GetExecutorsServer) error {
	ctx, cancel := reduceTimeout(srv.Context())
	defer cancel()
	executorClient, err := s.client.GetExecutors(ctx, &schedulerobjects.StreamingExecutorGetRequest{
		Num: 0,
	})
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

// We reduce the context deadline here, to prevent our call and the caller who called us from timing out at the same time
// This should mean our caller gets the real error message rather than a generic timeout error from client side
func reduceTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.WithCancel(ctx)
	}
	deadline = deadline.Add(-time.Second)
	return context.WithDeadline(ctx, deadline)
}
