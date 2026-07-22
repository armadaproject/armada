package client_test

import (
	"testing"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

func TestWithQueueServiceClient_FunctionSignatureExists(t *testing.T) {
	var _ func(*client.ApiConnectionDetails, func(api.QueueServiceClient) error) error = client.WithQueueServiceClient
	var _ func(*client.ApiConnectionDetails, func(api.JobsClient) error) error = client.WithJobsClient
}
