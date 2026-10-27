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
		Use:   "executor <executor-name>",
		Short: "Delete an executor from the scheduler database",
		Long:  "Deletes an executor from the scheduler database. This does not contact the executor itself.",
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if name == "" {
				return fmt.Errorf("executor name must be non-empty")
			}
			return a.DeleteExecutor(name)
		},
	}
	return cmd
}
