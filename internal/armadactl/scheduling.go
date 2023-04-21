package armadactl

import (
	"fmt"

	"github.com/gogo/protobuf/types"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/client"
)

func (a *App) GetSchedulingReport() error {
	return client.WithSchedulerReportingClient(a.Params.ApiConnectionDetails, func(c schedulerobjects.SchedulerReportingClient) error {
		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()
		report, err := c.GetSchedulingReport(ctx, &types.Empty{})
		if err != nil {
			return err
		}
		fmt.Fprint(a.Out, report.Report)
		return nil
	})
}

func (a *App) GetQueueSchedulingReport(queue string) error {
	return client.WithSchedulerReportingClient(a.Params.ApiConnectionDetails, func(c schedulerobjects.SchedulerReportingClient) error {
		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()
		report, err := c.GetQueueReport(ctx, &schedulerobjects.Queue{Name: queue})
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
		report, err := c.GetJobReport(ctx, &schedulerobjects.JobId{Id: jobId})
		if err != nil {
			return err
		}
		fmt.Fprint(a.Out, report.Report)
		return nil
	})
}
