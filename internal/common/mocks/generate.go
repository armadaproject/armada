package mocks

// Mock implementations used by tests
//go:generate mockgen -destination=./mock_pulsar.go -package=mocks "github.com/apache/pulsar-client-go/pulsar" Client,Producer,Message
//go:generate mockgen -destination=./mock_executorapi.go -package=mocks "github.com/armadaproject/armada/pkg/executorapi" ExecutorApiClient,ExecutorApi_LeaseJobRunsClient
//go:generate mockgen -destination=./controlplaneevents/mock_publisher.go -package=controlplaneevents "github.com/armadaproject/armada/internal/common/pulsarutils/controlplaneevents" Publisher
//go:generate mockgen -destination=./jobsetevents/mock_publisher.go -package=jobsetevents "github.com/armadaproject/armada/internal/common/pulsarutils/jobsetevents" Publisher
