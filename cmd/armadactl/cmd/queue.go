package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/cmd/armadactl/cmd/utils"
	"github.com/armadaproject/armada/internal/armadactl"
	"github.com/armadaproject/armada/internal/common/slices"
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

Job priority is evaluated inside queue, queue has its own priority.  Any labels on the queue must have a Kubernetes-like key-value structure, for example: armadaproject.io/submitter=airflow.`,
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

			cordoned, err := cmd.Flags().GetBool("cordon")
			if err != nil {
				return fmt.Errorf("error reading cordon: %s", err)
			}

			labels, err := cmd.Flags().GetStringSlice("labels")
			if err != nil {
				return fmt.Errorf("error reading queue labels: %s", err)
			}

			labelsAsMap, err := utils.LabelSliceAsMap(labels)
			if err != nil {
				return fmt.Errorf("error converting queue labels to map: %s", err)
			}

			newQueue, err := queue.NewQueue(&api.Queue{
				Name:           name,
				PriorityFactor: priorityFactor,
				UserOwners:     owners,
				GroupOwners:    groups,
				Cordoned:       cordoned,
				Labels:         labelsAsMap,
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
	cmd.Flags().Bool("cordon", false, "Used to pause scheduling on specified queue. Defaults to false.")
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
		Use:   "queues <queue_1> <queue_2> <queue_3> ...",
		Short: "Gets information from multiple queues.",
		Long:  "Gets information from multiple queues, filtering by either a set of queue names or a set of labels. Defaults to retrieving all queues.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, queues []string) error {
			errs := slices.Filter(slices.Map(queues, utils.QueueNameValidation), func(err error) bool { return err != nil })
			if len(errs) > 0 {
				return fmt.Errorf("provided queue name invalid: %s", errs[0])
			}

			onlyCordoned, err := cmd.Flags().GetBool("only-cordoned")
			if err != nil {
				return fmt.Errorf("error reading only-cordoned flag: %s", err)
			}

			inverse, err := cmd.Flags().GetBool("inverse")
			if err != nil {
				return fmt.Errorf("error reading inverse flag: %s", err)
			}

			labels, err := cmd.Flags().GetStringSlice("match-labels")
			if err != nil {
				return fmt.Errorf("error reading queue labels: %s", err)
			}

			if len(queues) > 0 && len(labels) > 0 {
				return fmt.Errorf("you can select either with a set of queue names or a set of queue labels, but not both")
			}

			return a.GetAllQueues(&armadactl.QueueQueryArgs{
				InQueueNames:      queues,
				ContainsAllLabels: labels,
				InvertResult:      inverse,
				OnlyCordoned:      onlyCordoned,
			})
		},
	}
	cmd.Flags().StringSliceP("match-labels", "l", []string{}, "Select queues by label.")
	cmd.Flags().Bool("inverse", false, "Inverts result to get all queues that don't match the specified criteria. Defaults to false.")
	cmd.Flags().Bool("only-cordoned", false, "Only returns queues that are cordoned. Defaults to false.")

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

			cordoned, err := cmd.Flags().GetBool("cordon")
			if err != nil {
				return fmt.Errorf("error reading cordon: %s", err)
			}

			labels, err := cmd.Flags().GetStringSlice("labels")
			if err != nil {
				return fmt.Errorf("error reading queue labels: %s", err)
			}

			labelsAsMap, err := utils.LabelSliceAsMap(labels)
			if err != nil {
				return fmt.Errorf("error converting queue labels to map: %s", err)
			}

			newQueue, err := queue.NewQueue(&api.Queue{
				Name:           name,
				PriorityFactor: priorityFactor,
				UserOwners:     owners,
				GroupOwners:    groups,
				Cordoned:       cordoned,
				Labels:         labelsAsMap,
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
	cmd.Flags().Bool("cordon", false, "Used to pause scheduling on specified queue. Defaults to false.")
	cmd.Flags().StringSliceP("labels", "l", []string{}, "Comma separated list of key-value queue labels, for example: armadaproject.io/submitter=airflow. Defaults to empty list.")
	return cmd
}
