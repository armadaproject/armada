package retrypolicy

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

type UpdateAPI func(policy *api.RetryPolicy) error

func Update(getConnectionDetails client.ConnectionDetails) UpdateAPI {
	return func(policy *api.RetryPolicy) error {
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

		c := api.NewRetryPolicyServiceClient(conn)
		if _, err = c.UpdateRetryPolicy(ctx, policy); err != nil {
			return fmt.Errorf("update retry policy request failed: %s", err)
		}
		return nil
	}
}
