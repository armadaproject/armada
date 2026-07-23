package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func executorDeleteCmd() *cobra.Command {
	return executorDeleteCmdWithApp(armadactl.New())
}

// Takes a caller-supplied app struct; useful for testing.
func executorDeleteCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "executor <executor_id>",
		Short: "Delete existing executor",
		Long:  "Deletes an executor if it exists.",
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			executorID := args[0]
			if executorID == "" {
				return fmt.Errorf("provided executor id is invalid: %s", executorID)
			}

			return a.DeleteExecutor(executorID)
		},
	}
	return cmd
}
