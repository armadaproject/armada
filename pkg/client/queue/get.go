package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

type GetAPI func(string) (*api.Queue, error)

func Get(getConnectionDetails client.ConnectionDetails) GetAPI {
	return func(queueName string) (*api.Queue, error) {
		conn, err := client.CreateApiConnection(getConnectionDetails())
		if err != nil {
			return nil, fmt.Errorf("failed to connect to api because %s", err)
		}
		defer conn.Close()

		client := api.NewSubmitClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		queue, err := client.GetQueue(ctx, &api.QueueGetRequest{Name: queueName})
		if err != nil {
			return nil, fmt.Errorf("get queue info request failed: %s", err)
		}

		return queue, nil
	}
}
