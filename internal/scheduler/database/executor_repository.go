package database

import (
	"time"

	"github.com/armadaproject/armada/pkg/executorapi"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// Executor is a representation of an Armada Executor
type Executor struct {
	// Name of the executor
	Name string
	// Pool that the executor belongs to
	Pool string
	// Current Per-Queue resource usage for this executor
	Usage *schedulerobjects.ClusterResourceUsageReport
	// The nodes available for scheduling via this executor
	Nodes []*schedulerobjects.Node
	// Minimum resources which a job must request in order to be considered for scheduling on this executor
	MinimumJobSize map[string]resource.Quantity
	// Last time the executor provided a heartbeat to say it was still accepting jobs
	LastUpdateTime time.Time
}

// ExecutorRepository is an interface to be implemented by structs which provide executor information
type ExecutorRepository interface {
	// GetExecutors returns all known executors, regardless of their last heartbeat time
	GetExecutors() ([]*Executor, error)
	// GetLastUpdateTimes returns a map of executor name -> last heartbeat time
	GetLastUpdateTimes() (map[string]time.Time, error)
	// StoreRequest persists the last lease request made by an executor
	StoreRequest(req *executorapi.LeaseRequest) error
}
