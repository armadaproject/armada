package node

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

type PreemptAPI func(string, string, []string, []string) error

func PreemptOnNode(getConnectionDetails client.ConnectionDetails) PreemptAPI {
	return func(name, executorName string, queues []string, priorityClasses []string) error {
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

		nodeClient := api.NewNodeClient(conn)
		newPreemptOnNode := &api.NodePreemptRequest{
			Name:            name,
			Executor:        executorName,
			Queues:          queues,
			PriorityClasses: priorityClasses,
		}
		if _, err = nodeClient.PreemptOnNode(ctx, newPreemptOnNode); err != nil {
			return err
		}
		return nil
	}
}
