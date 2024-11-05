package executor

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

type CancelAPI func(string, []string, []string) error

func CancelOnExecutor(getConnectionDetails client.ConnectionDetails) CancelAPI {
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
		newCancelOnExecutor := &api.ExecutorCancelRequest{
			Name:            executor,
			Queues:          queues,
			PriorityClasses: priorityClasses,
		}
		if _, err = executorClient.CancelOnExecutor(ctx, newCancelOnExecutor); err != nil {
			return err
		}
		return nil
	}
}
