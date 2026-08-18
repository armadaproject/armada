package cmd

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func reprioritiseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reprioritise",
		Short: "Reprioritise jobs in Armada",
		Long:  `Change the priority of a single job or entire job-set. Supported: job, job-set`,
	}
	cmd.AddCommand(
		reprioritiseJobCmd(),
		reprioritiseJobSetCmd(),
	)

	return cmd
}

func reprioritiseJobCmd() *cobra.Command {
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

			return a.ReprioritiseJob(queue, jobSet, jobId, priorityFactor)
		},
	}
	return cmd
}

func reprioritiseJobSetCmd() *cobra.Command {
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

			return a.ReprioritiseJobSet(queue, jobSet, priorityFactor)
		},
	}
	return cmd
}
