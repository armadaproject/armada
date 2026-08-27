package retrypolicy

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

type CreateAPI func(policy *api.RetryPolicy) error

func Create(getConnectionDetails client.ConnectionDetails) CreateAPI {
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
		if _, err := c.CreateRetryPolicy(ctx, policy); err != nil {
			return fmt.Errorf("create retry policy request failed: %s", err)
		}

		return nil
	}
}
