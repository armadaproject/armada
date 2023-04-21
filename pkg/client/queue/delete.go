package queue

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

type DeleteAPI func(string) error

func Delete(getConnectionDetails client.ConnectionDetails) DeleteAPI {
	return func(queueName string) error {
		conn, err := client.CreateApiConnection(getConnectionDetails())
		if err != nil {
			return fmt.Errorf("failed to connect to api because %s", err)
		}
		defer conn.Close()

		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()

		client := api.NewSubmitClient(conn)
		if _, err = client.DeleteQueue(ctx, &api.QueueDeleteRequest{Name: queueName}); err != nil {
			return fmt.Errorf("delete queue request failed: %s", err)
		}

		return nil
	}
}
