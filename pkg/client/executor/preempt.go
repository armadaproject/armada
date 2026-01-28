package executor

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// PreemptAPI preempts jobs on an executor matching the given queues, priority classes, and pools.
// Empty queues or pools means all queues or all pools respectively.
type PreemptAPI func(executor string, queues, priorityClasses, pools []string) error

func PreemptOnExecutor(getConnectionDetails client.ConnectionDetails) PreemptAPI {
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
		_, err = executorClient.PreemptOnExecutor(ctx, &api.ExecutorPreemptRequest{
			Name:            executor,
			Queues:          queues,
			PriorityClasses: priorityClasses,
			Pools:           pools,
		})
		return err
	}
}
