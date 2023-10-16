package cmd

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

func queueCreateCmd() *cobra.Command {
	return queueCreateCmdWithApp(armadactl.New())
}

// Takes a caller-supplied app struct; useful for testing.
func queueCreateCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queue <queueName>",
		Short: "Create new queue",
		Long: `Every job submitted to armada needs to be associated with queue.

Job priority is evaluated inside queue, queue has its own priority.`,
		Args: cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			// TODO cmd.Flags().GetFloat64("priorityFactor") returns (0, nil) for invalid input (e.g., "not_a_float")
			// TODO the other Flags get methods also fail to return errors on invalid input
			priorityFactor, err := cmd.Flags().GetFloat64("priorityFactor")
			if err != nil {
				return fmt.Errorf("error reading priorityFactor: %s", err)
			}

			owners, err := cmd.Flags().GetStringSlice("owners")
			if err != nil {
				return fmt.Errorf("error reading owners: %s", err)
			}

			groups, err := cmd.Flags().GetStringSlice("groupOwners")
			if err != nil {
				return fmt.Errorf("error reading groupOwners: %s", err)
			}

			resourceLimits, err := flagGetStringToString(cmd.Flags().GetStringToString).toFloat64("resourceLimits")
			if err != nil {
				return fmt.Errorf("error reading resourceLimits: %s", err)
			}

			queue, err := queue.NewQueue(&api.Queue{
				Name:           name,
				PriorityFactor: priorityFactor,
				UserOwners:     owners,
				GroupOwners:    groups,
				ResourceLimits: resourceLimits,
			})
			if err != nil {
				return fmt.Errorf("invalid queue data: %s", err)
			}

			return a.CreateQueue(queue)
		},
	}
	cmd.Flags().Float64("priorityFactor", 1, "Set queue priority factor - lower number makes queue more important, must be > 0.")
	cmd.Flags().StringSlice("owners", []string{}, "Comma separated list of queue owners, defaults to current user.")
	cmd.Flags().StringSlice("groupOwners", []string{}, "Comma separated list of queue group owners, defaults to empty list.")
	cmd.Flags().StringToString("resourceLimits", map[string]string{},
		"Command separated list of resource limits pairs, defaults to empty list.\nExample: --resourceLimits cpu=0.3,memory=0.2",
	)
	return cmd
}

func queueDeleteCmd() *cobra.Command {
	return queueDeleteCmdWithApp(armadactl.New())
}

// Takes a caller-supplied app struct; useful for testing.
func queueDeleteCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queue <queueName>",
		Short: "Delete existing queue",
		Long:  "Deletes queue if it exists, the queue needs to be empty at the time of deletion.",
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			return a.DeleteQueue(name)
		},
	}
	return cmd
}

func queueDescribeCmd() *cobra.Command {
	return queueDescribeCmdWithApp(armadactl.New())
}

// Takes a caller-supplied app struct; useful for testing.
func queueDescribeCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queue <queueName>",
		Short: "Prints out queue info.",
		Long:  "Prints out queue info including all jobs sets where jobs are running or queued.",
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			return a.DescribeQueue(name)
		},
	}
	return cmd
}

func queueGetCmd() *cobra.Command {
	return queueDescribeCmdWithApp(armadactl.New())
}

// Takes a caller-supplied app struct; useful for testing.
func queueGetCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queue <queueName>",
		Short: "Gets Queue Information.",
		Long:  "Gets Queue Information",
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			return a.GetQueue(name)
		},
	}
	return cmd
}

func queueUpdateCmd() *cobra.Command {
	return queueUpdateCmdWithApp(armadactl.New())
}

// Takes a caller-supplied app struct; useful for testing.
func queueUpdateCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queue <queueName>",
		Short: "Update an existing queue",
		Long:  "Update settings of an existing queue",
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			priorityFactor, err := cmd.Flags().GetFloat64("priorityFactor")
			if err != nil {
				return fmt.Errorf("error reading priorityFactor: %s", err)
			}

			owners, err := cmd.Flags().GetStringSlice("owners")
			if err != nil {
				return fmt.Errorf("error reading owners: %s", err)
			}

			groups, err := cmd.Flags().GetStringSlice("groupOwners")
			if err != nil {
				return fmt.Errorf("error reading groupOwners: %s", err)
			}

			resourceLimits, err := flagGetStringToString(cmd.Flags().GetStringToString).toFloat64("resourceLimits")
			if err != nil {
				return fmt.Errorf("error reading resourceLimits: %s", err)
			}

			queue, err := queue.NewQueue(&api.Queue{
				Name:           name,
				PriorityFactor: priorityFactor,
				UserOwners:     owners,
				GroupOwners:    groups,
				ResourceLimits: resourceLimits,
			})
			if err != nil {
				return fmt.Errorf("invalid queue data: %s", err)
			}

			return a.UpdateQueue(queue)
		},
	}
	// TODO this will overwrite existing values with default values if not all flags are provided
	cmd.Flags().Float64("priorityFactor", 1, "Set queue priority factor - lower number makes queue more important, must be > 0.")
	cmd.Flags().StringSlice("owners", []string{}, "Comma separated list of queue owners, defaults to current user.")
	cmd.Flags().StringSlice("groupOwners", []string{}, "Comma separated list of queue group owners, defaults to empty list.")
	cmd.Flags().StringToString("resourceLimits", map[string]string{},
		"Command separated list of resource limits pairs, defaults to empty list. Example: --resourceLimits cpu=0.3,memory=0.2",
	)
	return cmd
}

type flagGetStringToString func(string) (map[string]string, error)

func (f flagGetStringToString) toFloat64(flagName string) (map[string]float64, error) {
	limits, err := f(flagName)
	if err != nil {
		return nil, err
	}

	result := make(map[string]float64, len(limits))
	for resourceName, limit := range limits {
		limitFloat, err := strconv.ParseFloat(limit, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s as float64. %s", resourceName, err)
		}
		result[resourceName] = limitFloat
	}

	return result, nil
}
