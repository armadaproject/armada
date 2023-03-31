package cmd

import (
	"strings"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func getSchedulingReportCmd(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "scheduling-report",
		Short:        "Get scheduler reports",
		Args:         cobra.ExactArgs(0),
		SilenceUsage: true,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.GetSchedulingReport()
		},
	}
	return cmd
}

func getQueueSchedulingReportCmd(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "queue-report",
		Short:        "Get queue scheduler reports",
		Args:         cobra.ExactArgs(0),
		SilenceUsage: true,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			queue, err := cmd.Flags().GetString("queue")
			if err != nil {
				return err
			}
			queue = strings.TrimSpace(queue)
			return a.GetQueueSchedulingReport(queue)
		},
	}
	cmd.Flags().String("queue", "", "Queue name to query reports for.")
	return cmd
}

func getJobSchedulingReportCmd(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "job-report",
		Short:        "Get job scheduler reports",
		Args:         cobra.ExactArgs(0),
		SilenceUsage: true,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			jobId, err := cmd.Flags().GetString("jobId")
			if err != nil {
				return err
			}
			jobId = strings.TrimSpace(jobId)
			return a.GetJobSchedulingReport(jobId)
		},
	}
	cmd.Flags().String("jobId", "", "Id of job to query reports for.")
	return cmd
}
