package cmd

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func reprioritizeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reprioritize",
		Short: "Reprioritize jobs in Armada",
		Long:  `Change the priority of a single job or entire job-set. Supported: job, job-set`,
	}
	cmd.AddCommand(
		reprioritizeJobCmd(),
		reprioritizeJobSetCmd(),
	)

	return cmd
}

func reprioritizeJobCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "job <queue> <job-set> <job-id> <priority>",
		Short: `Change the priority of a single job.`,
		Args:  cobra.ExactArgs(4),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// Ignoring first two arguments until Server API change makes queue and job-set a requirement
			queue := args[0]
			jobSet := args[1]
			jobId := args[2]
			priorityString := args[3]
			priorityFactor, err := strconv.ParseFloat(priorityString, 64)
			if err != nil {
				return fmt.Errorf("error converting %s to float64: %s", priorityString, err)
			}

			return a.ReprioritizeJob(queue, jobSet, jobId, priorityFactor)
		},
	}
	return cmd
}

func reprioritizeJobSetCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "job-set <queue> <job-set> <priority>",
		Short: `Change the priority of an entire job set.`,
		Args:  cobra.ExactArgs(3),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			queue := args[0]
			jobSet := args[1]

			priorityString := args[2]
			priorityFactor, err := strconv.ParseFloat(priorityString, 64)
			if err != nil {
				return fmt.Errorf("error converting %s to float64: %s", priorityString, err)
			}

			return a.ReprioritizeJobSet(queue, jobSet, priorityFactor)
		},
	}
	return cmd
}
