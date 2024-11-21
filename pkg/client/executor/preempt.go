package executor

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

type PreemptAPI func(string, []string, []string) error

func PreemptOnExecutor(getConnectionDetails client.ConnectionDetails) PreemptAPI {
	return func(executor string, queues []string, priorityClasses []string) error {
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
		newPreemptOnExecutor := &api.ExecutorPreemptRequest{
			Name:            executor,
			Queues:          queues,
			PriorityClasses: priorityClasses,
		}
		if _, err = executorClient.PreemptOnExecutor(ctx, newPreemptOnExecutor); err != nil {
			return err
		}
		return nil
	}
}
