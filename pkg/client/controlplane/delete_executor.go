package controlplane

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

type DeleteExecutorAPI func(string) error

func DeleteExecutor(getConnectionDetails client.ConnectionDetails) DeleteExecutorAPI {
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

		submitClient := api.NewSubmitClient(conn)
		if _, err = submitClient.DeleteExecutor(ctx, &api.ExecutorDeleteRequest{Name: executor}); err != nil {
			return fmt.Errorf("delete executor request failed: %s", err)
		}

		return nil
	}
}
