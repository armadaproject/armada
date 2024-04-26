package schedulermocks

// Mock implementations used by scheduler tests
//go:generate mockgen -destination=./leases_getter.go -package=schedulermocks "k8s.io/client-go/kubernetes/typed/coordination/v1" LeasesGetter,LeaseInterface
//go:generate mockgen -destination=./job_repository.go -package=schedulermocks "github.com/armadaproject/armada/internal/scheduler/database" JobRepository
//go:generate mockgen -destination=./executor_repository.go -package=schedulermocks "github.com/armadaproject/armada/internal/scheduler/database" ExecutorRepository
//go:generate mockgen -destination=./queue_repository.go -package=schedulermocks "github.com/armadaproject/armada/internal/armada/repository" QueueRepository
//go:generate mockgen -destination=./grpc.go -package=schedulermocks "github.com/armadaproject/armada/pkg/executorapi" ExecutorApi_LeaseJobRunsServer
