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
			verbosity, err := cmd.Flags().GetCount("verbose")
			if err != nil {
				return err
			}

			queueName, err := cmd.Flags().GetString("queue")
			if err != nil {
				return err
			}
			queueName = strings.TrimSpace(queueName)
			if queueName != "" {
				return a.GetSchedulingReportForQueue(queueName, int32(verbosity))
			}

			jobId, err := cmd.Flags().GetString("job")
			if err != nil {
				return err
			}
			jobId = strings.TrimSpace(jobId)
			if jobId != "" {
				return a.GetSchedulingReportForJob(jobId, int32(verbosity))
			}

			return a.GetSchedulingReport(int32(verbosity))
		},
	}

	cmd.Flags().CountP("verbose", "v", "report verbosity; repeat (e.g., -vvv) to increase verbosity")

	cmd.Flags().String("queue", "", "get scheduler reports relevant for this queue; mutually exclusive with --job")
	cmd.Flags().String("job", "", "get scheduler reports relevant for this job; mutually exclusive with --queue")
	cmd.MarkFlagsMutuallyExclusive("queue", "job")

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
