package queue

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

type CordonAPI func(string) error

func Cordon(getConnectionDetails client.ConnectionDetails) CordonAPI {
	return func(queueName string) error {
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
		if _, err = queueClient.CordonQueue(ctx, &api.QueueCordonRequest{Name: queueName}); err != nil {
			return err
		}
		return nil
	}
}

type UncordonAPI func(string) error

func Uncordon(getConnectionDetails client.ConnectionDetails) UncordonAPI {
	return func(queueName string) error {
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
		if _, err = queueClient.UncordonQueue(ctx, &api.QueueUncordonRequest{Name: queueName}); err != nil {
			return err
		}
		return nil
	}
}
