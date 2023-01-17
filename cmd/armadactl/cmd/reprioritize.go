package cmd

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func reprioritizeCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "reprioritize <priority>",
		Short: "Reprioritize jobs in Armada",
		Long:  `Change the priority of a single or multiple jobs by specifying either a job id or a combination of queue & job set.`,
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			priorityString := args[0]
			priorityFactor, err := strconv.ParseFloat(priorityString, 64)
			if err != nil {
				return fmt.Errorf("error converting %s to float64: %s", priorityString, err)
			}

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

			return a.Reprioritize(jobId, queueName, jobSetId, priorityFactor)
		},
	}
	cmd.Flags().String("jobId", "", "Job to reprioritize")
	cmd.Flags().String("queue", "", "Queue including jobs to be reprioritized (requires job set to be specified)")
	cmd.Flags().String("jobSet", "", "Job set including jobs to be reprioritized (requires queue to be specified)")
	return cmd
}
