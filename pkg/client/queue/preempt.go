package queue

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

type PreemptAPI func(string, []string) error

func Preempt(getConnectionDetails client.ConnectionDetails) PreemptAPI {
	return func(queueName string, priorityClasses []string) error {
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
		if _, err = queueClient.PreemptOnQueue(ctx, &api.QueuePreemptRequest{Name: queueName, PriorityClasses: priorityClasses}); err != nil {
			return err
		}
		return nil
	}
}
