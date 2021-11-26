package queue

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"github.com/G-Research/armada/pkg/client/queue"
)

func Describe(getQueueInfo queue.GetInfoAPI) *cobra.Command {
	command := &cobra.Command{
		Use:   "queue <queueName>",
		Short: "Prints out queue info.",
		Long:  "Prints out queue info including all jobs sets where jobs are running or queued.",
		Args:  validateQueueName,
	}

	command.RunE = func(cmd *cobra.Command, args []string) error {
		queueName := args[0]

		cmd.Printf("Queue %s:", queueName)
		queueInfo, err := getQueueInfo(queueName)
		if err != nil {
			return fmt.Errorf("failed to describe queue: %s. %s", queueName, err)
		}

		jobSets := queueInfo.ActiveJobSets
		sort.SliceStable(jobSets, func(i, j int) bool {
			return jobSets[i].Name < jobSets[j].Name
		})

		if len(jobSets) == 0 {
			cmd.Printf("No job queued or running.")
			return nil
		}

		for _, jobSet := range jobSets {
			cmd.Printf("in cluster: %d, queued: %d - %s", jobSet.LeasedJobs, jobSet.QueuedJobs, jobSet.Name)
		}
		return nil
	}

	return command
}
