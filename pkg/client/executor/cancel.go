package executor

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// CancelAPI cancels jobs on an executor matching the given queues, priority classes, and pools.
// Empty queues or pools means all queues or all pools respectively.
type CancelAPI func(executor string, queues, priorityClasses, pools []string) error

func CancelOnExecutor(getConnectionDetails client.ConnectionDetails) CancelAPI {
	return func(executor string, queues []string, priorityClasses []string, pools []string) error {
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

		executorClient := api.NewExecutorClient(conn)
		_, err = executorClient.CancelOnExecutor(ctx, &api.ExecutorCancelRequest{
			Name:            executor,
			Queues:          queues,
			PriorityClasses: priorityClasses,
			Pools:           pools,
		})
		return err
	}
}
