package armadactl

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/client"
)

func (a *App) executeGetSchedulingReport(request *schedulerobjects.SchedulingReportRequest) error {
	return client.WithSchedulerReportingClient(a.Params.ApiConnectionDetails, func(c schedulerobjects.SchedulerReportingClient) error {
		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()
		report, err := c.GetSchedulingReport(ctx, request)
		if err != nil {
			return err
		}
		fmt.Fprint(a.Out, report.Report)
		return nil
	})
}

func (a *App) GetSchedulingReportForQueue(queueName string, verbosity int32) error {
	return a.executeGetSchedulingReport(
		&schedulerobjects.SchedulingReportRequest{
			Filter: &schedulerobjects.SchedulingReportRequest_MostRecentForQueue{
				MostRecentForQueue: &schedulerobjects.MostRecentForQueue{
					QueueName: queueName,
				},
			},

			Verbosity: verbosity,
		},
	)
}

func (a *App) GetSchedulingReportForJob(jobId string, verbosity int32) error {
	return a.executeGetSchedulingReport(
		&schedulerobjects.SchedulingReportRequest{
			Filter: &schedulerobjects.SchedulingReportRequest_MostRecentForJob{
				MostRecentForJob: &schedulerobjects.MostRecentForJob{
					JobId: jobId,
				},
			},

			Verbosity: verbosity,
		},
	)
}

func (a *App) GetSchedulingReport(verbosity int32) error {
	return a.executeGetSchedulingReport(
		&schedulerobjects.SchedulingReportRequest{
			Verbosity: verbosity,
		},
	)
}

func (a *App) GetQueueSchedulingReport(queueName string, verbosity int32) error {
	return client.WithSchedulerReportingClient(a.Params.ApiConnectionDetails, func(c schedulerobjects.SchedulerReportingClient) error {
		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()
		report, err := c.GetQueueReport(ctx, &schedulerobjects.QueueReportRequest{QueueName: queueName, Verbosity: verbosity})
		if err != nil {
			return err
		}
		fmt.Fprint(a.Out, report.Report)
		return nil
	})
}

func (a *App) GetJobSchedulingReport(jobId string) error {
	return client.WithSchedulerReportingClient(a.Params.ApiConnectionDetails, func(c schedulerobjects.SchedulerReportingClient) error {
		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()
		report, err := c.GetJobReport(ctx, &schedulerobjects.JobReportRequest{JobId: jobId})
		if err != nil {
			return err
		}
		fmt.Fprint(a.Out, report.Report)
		return nil
	})
}

func (a *App) GetExecutors() error {
	return client.WithSchedulerReportingClient(a.Params.ApiConnectionDetails, func(c schedulerobjects.SchedulerReportingClient) error {
		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()
		executorClient, err := c.GetExecutors(ctx, &schedulerobjects.StreamingExecutorGetRequest{
			Num: 0,
		})
		fmt.Fprint(a.Out, "Available executors:\n")
		defer func() { _ = executorClient.CloseSend() }()
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
				switch msg.Event.(type) {
				case *schedulerobjects.StreamingExecutorMessage_Executor:
					fmt.Fprintf(a.Out, "%s\n", msg.Event.(*schedulerobjects.StreamingExecutorMessage_Executor).Executor.Id)
				case *schedulerobjects.StreamingExecutorMessage_End:
					return nil
				default:
				}
			}
		}
	})
}
