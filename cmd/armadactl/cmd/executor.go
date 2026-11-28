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

func executorGetCmd() *cobra.Command {
	return executorGetCmdWithApp(armadactl.New())
}

// Takes a caller-supplied app struct; useful for testing.
func executorGetCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "executor <executor-name>",
		Short: "Get an executor known to the scheduler",
		Long:  "Gets an executor known to the scheduler, with its pool, last heartbeat, node count and cordon state.",
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// The name is used as given, since executor ids are stored unchanged.
			if args[0] == "" {
				return fmt.Errorf("executor name must be non-empty")
			}
			return a.GetExecutor(args[0])
		},
	}
	return withOutputFlag(cmd, a)
}

func executorsGetCmd() *cobra.Command {
	return executorsGetCmdWithApp(armadactl.New())
}

// Takes a caller-supplied app struct; useful for testing.
func executorsGetCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "executors",
		Short: "Get all executors known to the scheduler",
		Long:  "Gets all executors known to the scheduler, with their pool, last heartbeat, node count and cordon state.",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.GetAllExecutors()
		},
	}
	return withOutputFlag(cmd, a)
}
