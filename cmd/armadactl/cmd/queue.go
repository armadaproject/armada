package cmd

import (
	"fmt"

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
		Use:   "queue <queue-name>",
		Short: "Create new queue",
		Long: `Every job submitted to armada needs to be associated with queue.

Job priority is evaluated inside queue, queue has its own priority. Any labels on the queue must have a Kubernetes-like key-value structure, for example: armadaproject.io/submitter=airflow.`,
		Args: cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			// TODO cmd.Flags().GetFloat64("priority-factor") returns (0, nil) for invalid input (e.g., "not_a_float")
			// TODO the other Flags get methods also fail to return errors on invalid input
			priorityFactor, err := cmd.Flags().GetFloat64("priority-factor")
			if err != nil {
				return fmt.Errorf("error reading priority-factor: %s", err)
			}

			owners, err := cmd.Flags().GetStringSlice("owners")
			if err != nil {
				return fmt.Errorf("error reading owners: %s", err)
			}

			groups, err := cmd.Flags().GetStringSlice("group-owners")
			if err != nil {
				return fmt.Errorf("error reading group-owners: %s", err)
			}

			schedulingPaused, err := cmd.Flags().GetBool("pause-scheduling")
			if err != nil {
				return fmt.Errorf("error reading pause-scheduling: %s", err)
			}

			labels, err := cmd.Flags().GetStringSlice("labels")
			if err != nil {
				return fmt.Errorf("error reading queue labels: %s", err)
			}

			newQueue, err := queue.NewQueue(&api.Queue{
				Name:             name,
				PriorityFactor:   priorityFactor,
				UserOwners:       owners,
				GroupOwners:      groups,
				SchedulingPaused: schedulingPaused,
				Labels:           labels,
			})
			if err != nil {
				return fmt.Errorf("invalid queue data: %s", err)
			}

			return a.CreateQueue(newQueue)
		},
	}
	cmd.Flags().Float64("priority-factor", 1, "Set queue priority factor - lower number makes queue more important, must be > 0.")
	cmd.Flags().StringSlice("owners", []string{}, "Comma separated list of queue owners, defaults to current user.")
	cmd.Flags().StringSlice("group-owners", []string{}, "Comma separated list of queue group owners, defaults to empty list.")
	cmd.Flags().Bool("pause-scheduling", false, "Used to pause scheduling on specified queue. Defaults to false.")
	cmd.Flags().StringSliceP("labels", "l", []string{}, "Comma separated list of key-value queue labels, for example: armadaproject.io/submitter=airflow. Defaults to empty list.")
	return cmd
}

func queueDeleteCmd() *cobra.Command {
	return queueDeleteCmdWithApp(armadactl.New())
}

// Takes a caller-supplied app struct; useful for testing.
func queueDeleteCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queue <queue-name>",
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

func queueGetCmd() *cobra.Command {
	return queueGetCmdWithApp(armadactl.New())
}

// Takes a caller-supplied app struct; useful for testing.
func queueGetCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queue <queue-name>",
		Short: "Gets queue information.",
		Long:  "Gets queue information",
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

func queuesGetCmd() *cobra.Command {
	return queuesGetCmdWithApp(armadactl.New())
}

// Takes a caller-supplied app struct; useful for testing.
func queuesGetCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queues",
		Short: "Gets information from multiple queues.",
		Long:  "Gets information from multiple queues, filtering by queue name and/or matching labels. Defaults to retrieving all queues.",
		Args:  cobra.ExactArgs(0),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			inverse, err := cmd.Flags().GetBool("inverse")
			if err != nil {
				return fmt.Errorf("error reading inverse flag: %s", err)
			}

			labels, err := cmd.Flags().GetStringSlice("match-labels")
			if err != nil {
				return fmt.Errorf("error reading queue labels: %s", err)
			}

			queues, err := cmd.Flags().GetStringSlice("match-queues")
			if err != nil {
				return fmt.Errorf("error reading queue labels: %s", err)
			}

			return a.GetAllQueues(queues, labels, inverse)
		},
	}
	cmd.Flags().StringSliceP("match-queues", "q", []string{}, "Select queues matching provided queue names.")
	cmd.Flags().StringSliceP("match-labels", "l", []string{}, "Select queues by label.")
	cmd.Flags().Bool("inverse", false, "Inverts result to get all queues which don't contain provided set of labels. Defaults to false.")

	return cmd
}

func queueUpdateCmd() *cobra.Command {
	return queueUpdateCmdWithApp(armadactl.New())
}

// Takes a caller-supplied app struct; useful for testing.
func queueUpdateCmdWithApp(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queue <queue-name>",
		Short: "Update an existing queue",
		Long:  "Update settings of an existing queue",
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			priorityFactor, err := cmd.Flags().GetFloat64("priority-factor")
			if err != nil {
				return fmt.Errorf("error reading priority-factor: %s", err)
			}

			owners, err := cmd.Flags().GetStringSlice("owners")
			if err != nil {
				return fmt.Errorf("error reading owners: %s", err)
			}

			groups, err := cmd.Flags().GetStringSlice("group-owners")
			if err != nil {
				return fmt.Errorf("error reading group-owners: %s", err)
			}

			schedulingPaused, err := cmd.Flags().GetBool("pause-scheduling")
			if err != nil {
				return fmt.Errorf("error reading pause-scheduling: %s", err)
			}

			labels, err := cmd.Flags().GetStringSlice("labels")
			if err != nil {
				return fmt.Errorf("error reading queue labels: %s", err)
			}

			newQueue, err := queue.NewQueue(&api.Queue{
				Name:             name,
				PriorityFactor:   priorityFactor,
				UserOwners:       owners,
				GroupOwners:      groups,
				SchedulingPaused: schedulingPaused,
				Labels:           labels,
			})
			if err != nil {
				return fmt.Errorf("invalid queue data: %s", err)
			}

			return a.UpdateQueue(newQueue)
		},
	}
	// TODO this will overwrite existing values with default values if not all flags are provided
	cmd.Flags().Float64("priority-factor", 1, "Set queue priority factor - lower number makes queue more important, must be > 0.")
	cmd.Flags().StringSlice("owners", []string{}, "Comma separated list of queue owners, defaults to current user.")
	cmd.Flags().StringSlice("group-owners", []string{}, "Comma separated list of queue group owners, defaults to empty list.")
	cmd.Flags().Bool("pause-scheduling", false, "Used to pause scheduling on specified queue. Defaults to false.")
	cmd.Flags().StringSliceP("labels", "l", []string{}, "Comma separated list of key-value queue labels, for example: armadaproject.io/submitter=airflow. Defaults to empty list.")
	return cmd
}
