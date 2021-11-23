package queue

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/G-Research/armada/pkg/client/queue"
)

func Delete(deleteQueue queue.DeleteAPI) *cobra.Command {
	command := &cobra.Command{
		Use:          "queue <queueName>",
		Short:        "Delete existing queue",
		Long:         "Deletes queue if it exists, the queue needs to be empty at the time of deletion.",
		SilenceUsage: true,
		Args:         validateQueueName,
	}

	command.RunE = func(cmd *cobra.Command, args []string) error {
		queueName := args[0]

		if err := deleteQueue(queueName); err != nil {
			return fmt.Errorf("failed to delete queue with name: %s. %s", queueName, err)
		}

		cmd.Printf("Queue %s deleted or did not exist.", queueName)
		return nil
	}

	return command
}
