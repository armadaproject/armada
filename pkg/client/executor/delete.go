package executor

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// DeleteAPI deletes an executor from the scheduler database.
type DeleteAPI func(executor string) error

func DeleteExecutor(getConnectionDetails client.ConnectionDetails) DeleteAPI {
	return func(executor string) error {
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
		_, err = executorClient.DeleteExecutor(ctx, &api.ExecutorDeleteRequest{Name: executor})
		return err
	}
}
