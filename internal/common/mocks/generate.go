package mocks

// Mock implementations used by tests
//go:generate mockgen -destination=./mock_pulsar.go -package=mocks "github.com/apache/pulsar-client-go/pulsar" Client,Producer,Message
//go:generate mockgen -destination=./mock_publisher.go -package=mocks "github.com/armadaproject/armada/internal/common/pulsarutils" Publisher
//go:generate mockgen -destination=./mock_executorapi.go -package=mocks "github.com/armadaproject/armada/pkg/executorapi" ExecutorApiClient,ExecutorApi_LeaseJobRunsClient
