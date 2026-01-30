package queue

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// PreemptAPI preempts jobs on a queue.
// Parameters: queue name, priorityClasses, pools (empty=all)
type PreemptAPI func(queueName string, priorityClasses, pools []string) error

func Preempt(getConnectionDetails client.ConnectionDetails) PreemptAPI {
	return func(queueName string, priorityClasses, pools []string) error {
		connectionDetails, err := getConnectionDetails()
		if err != nil {
			return fmt.Errorf("failed to obtain api connection details: %s", err)
		}
		conn, err := client.CreateApiConnection(connectionDetails)
		if err != nil {
			return fmt.Errorf("failed to connect to api because %s", err)
		}
		defer conn.Close()

		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()

		queueClient := api.NewQueueServiceClient(conn)
		_, err = queueClient.PreemptOnQueue(ctx, &api.QueuePreemptRequest{Name: queueName, PriorityClasses: priorityClasses, Pools: pools})
		return err
	}
}
