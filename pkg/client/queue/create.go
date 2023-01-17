package queue

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

type CreateAPI func(Queue) error

func Create(getConnectionDetails client.ConnectionDetails) CreateAPI {
	return func(queue Queue) error {
		conn, err := client.CreateApiConnection(getConnectionDetails())
		if err != nil {
			return fmt.Errorf("failed to connect to api because %s", err)
		}
		defer conn.Close()

		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()

		client := api.NewSubmitClient(conn)
		if _, err := client.CreateQueue(ctx, queue.ToAPI()); err != nil {
			return fmt.Errorf("create queue request failed: %s", err)
		}

		return nil
	}
}
