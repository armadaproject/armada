package executor

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api/schedulerobjects"
	"github.com/armadaproject/armada/pkg/client"
)

// GetAPI returns the executors known to the scheduler. If name is non-empty, only that executor is returned.
type GetAPI func(name string) ([]*schedulerobjects.ExecutorSummary, error)

func GetExecutors(getConnectionDetails client.ConnectionDetails) GetAPI {
	return func(name string) ([]*schedulerobjects.ExecutorSummary, error) {
		connectionDetails, err := getConnectionDetails()
		if err != nil {
			return nil, fmt.Errorf("failed to obtain api connection details: %s", err)
		}
		conn, err := client.CreateApiConnection(connectionDetails)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to api because %s", err)
		}
		defer conn.Close()

		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()

		reportingClient := schedulerobjects.NewSchedulerReportingClient(conn)
		resp, err := reportingClient.GetExecutors(ctx, &schedulerobjects.ExecutorsRequest{Name: name})
		if err != nil {
			return nil, err
		}
		return resp.Executors, nil
	}
}
