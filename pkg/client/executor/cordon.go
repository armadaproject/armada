package executor

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

type CordonAPI func(string, string) error

func CordonExecutor(getConnectionDetails client.ConnectionDetails) CordonAPI {
	return func(executor string, cordonReason string) error {
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
		newExecutorSettings := &api.ExecutorSettingsUpsertRequest{
			Name:         executor,
			Cordoned:     true,
			CordonReason: cordonReason,
		}
		if _, err = executorClient.UpsertExecutorSettings(ctx, newExecutorSettings); err != nil {
			return err
		}
		return nil
	}
}

type UncordonAPI func(string) error

func UncordonExecutor(getConnectionDetails client.ConnectionDetails) UncordonAPI {
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
		newExecutorSettings := &api.ExecutorSettingsUpsertRequest{
			Name:         executor,
			Cordoned:     false,
			CordonReason: "",
		}
		if _, err = executorClient.UpsertExecutorSettings(ctx, newExecutorSettings); err != nil {
			return err
		}
		return nil
	}
}
