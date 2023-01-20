package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func kubeCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "kube",
		Short: "output kubectl command to access pod information",
		Long:  "This command can be used to query kubernetes pods for a particular job.",
		Example: `armadactl kube logs --queue my-queue --jobSet my-set --jobId 123456
		
In bash, you can execute it directly like this:
	$(armadactl kube logs --queue my-queue --jobSet my-set --jobId 123456) --tail=20`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			jobId, err := cmd.Flags().GetString("jobId")
			if err != nil {
				return fmt.Errorf("error reading jobId: %s", err)
			}

			queueName, err := cmd.Flags().GetString("queue")
			if err != nil {
				return fmt.Errorf("error reading queueName: %s", err)
			}

			jobSetId, err := cmd.Flags().GetString("jobSet")
			if err != nil {
				return fmt.Errorf("error reading jobSet: %s", err)
			}

			podNumber, err := cmd.Flags().GetInt("podNumber")
			if err != nil {
				return fmt.Errorf("error reading podNumber: %s", err)
			}

			return a.Kube(jobId, queueName, jobSetId, podNumber, args)
		},
	}
	cmd.Flags().String(
		"jobId", "", "job to cancel")
	if err := cmd.MarkFlagRequired("jobId"); err != nil {
		panic(err)
	}
	cmd.Flags().String(
		"queue", "", "queue of the job")
	if err := cmd.MarkFlagRequired("queue"); err != nil {
		panic(err)
	}
	cmd.Flags().String(
		"jobSet", "", "jobSet of the job")
	if err := cmd.MarkFlagRequired("jobSet"); err != nil {
		panic(err)
	}
	cmd.Flags().Int(
		"podNumber", 0, "[optional] for jobs with multiple pods, index of the pod")
	cmd.FParseErrWhitelist.UnknownFlags = true
	return cmd
}
