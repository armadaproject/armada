package queue

import (
	"fmt"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

type UpdateAPI func(api.Queue) error

func Update(getConnectionDetails client.ConnectionDetails) UpdateAPI {
	return func(queue api.Queue) error {
		conn, err := client.CreateApiConnection(getConnectionDetails())
		if err != nil {
			return fmt.Errorf("failed to connect to api because %s", err)
		}
		defer conn.Close()

		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()

		client := api.NewSubmitClient(conn)
		if _, err = client.UpdateQueue(ctx, &queue); err != nil {
			return fmt.Errorf("failed to create queue with name %s. %s", queue.Name, err)
		}
		return nil
	}
}
