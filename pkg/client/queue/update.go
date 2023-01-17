package queue

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

type UpdateAPI func(queue Queue) error

func Update(getConnectionDetails client.ConnectionDetails) UpdateAPI {
	return func(queue Queue) error {
		conn, err := client.CreateApiConnection(getConnectionDetails())
		if err != nil {
			return fmt.Errorf("failed to connect to api because %s", err)
		}
		defer conn.Close()

		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()

		client := api.NewSubmitClient(conn)
		if _, err = client.UpdateQueue(ctx, queue.ToAPI()); err != nil {
			return fmt.Errorf("failed to create queue with name %s. %s", queue.Name, err)
		}
		return nil
	}
}
