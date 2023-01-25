package schedulermocks

// Mock implementations used by scheduler tests
//go:generate mockgen -destination=./mock_pulsar.go -package=schedulermocks "github.com/apache/pulsar-client-go/pulsar" Client,Producer
//go:generate mockgen -destination=./mock_leases_getter.go -package=schedulermocks "k8s.io/client-go/kubernetes/typed/coordination/v1" LeasesGetter,LeaseInterface
//go:generate mockgen -destination=./mock_repositories.go -package=schedulermocks "github.com/armadaproject/armada/internal/scheduler/database" ExecutorRepository,QueueRepository,JobRepository
//go:generate mockgen -destination=./mock_grpc.go -package=schedulermocks "github.com/armadaproject/armada/pkg/executorapi" ExecutorApi_LeaseJobRunsServer
