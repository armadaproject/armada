package retrypolicy

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

type GetAPI func(name string) (*api.RetryPolicy, error)

func Get(getConnectionDetails client.ConnectionDetails) GetAPI {
	return func(name string) (*api.RetryPolicy, error) {
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

		c := api.NewRetryPolicyServiceClient(conn)
		policy, err := c.GetRetryPolicy(ctx, &api.RetryPolicyGetRequest{Name: name})
		if err != nil {
			return nil, fmt.Errorf("get retry policy request failed: %s", err)
		}

		return policy, nil
	}
}

type GetAllAPI func() ([]*api.RetryPolicy, error)

func GetAll(getConnectionDetails client.ConnectionDetails) GetAllAPI {
	return func() ([]*api.RetryPolicy, error) {
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

		c := api.NewRetryPolicyServiceClient(conn)
		list, err := c.GetRetryPolicies(ctx, &api.RetryPolicyListRequest{})
		if err != nil {
			return nil, fmt.Errorf("get retry policies request failed: %s", err)
		}

		return list.RetryPolicies, nil
	}
}
