package queue

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// CancelAPI cancels jobs on a queue.
// Parameters: queue name, priorityClasses, jobStates, pools (empty=all)
type CancelAPI func(queueName string, priorityClasses []string, jobStates []api.JobState, pools []string) error

func Cancel(getConnectionDetails client.ConnectionDetails) CancelAPI {
	return func(queueName string, priorityClasses []string, jobStates []api.JobState, pools []string) error {
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
		_, err = queueClient.CancelOnQueue(ctx, &api.QueueCancelRequest{Name: queueName, PriorityClasses: priorityClasses, JobStates: jobStates, Pools: pools})
		return err
	}
}
