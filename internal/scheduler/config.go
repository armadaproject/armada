package scheduler

import (
	"time"

	"github.com/armadaproject/armada/internal/armada/configuration"
)

type Configuration struct {
	// Database configuration
	Postgres configuration.PostgresConfig
	// General Pulsar configuration
	Pulsar configuration.PulsarConfig
	// Configuration controlling leader election
	Lease LeaderConfig
	// Scheduler configuration (this is shared with the old scheduler)
	Scheduling configuration.SchedulingConfig
	// How often the scheduling cycle should run
	cyclePeriod time.Duration
	// How long after a heartbeat an executor will be considered lost
	executorTimeout time.Duration
	// Maximum number of times a lease can be returned before the job is failed
	MaxFailedLeaseReturns int
	// Maximum number of rows to fetch in a given query
	DatabaseFetchSize int
	// Timeout to use when sending messages to pulsar
	PulsarSendTimeout time.Duration
}

type LeaderConfig struct {
	// Name of the K8s Lock Object
	LeaseLockName string
	// Namespace of the K8s Lock Object
	LeaseLockNamespace string
	// The name of the pod
	PodName string
	// How long the lease is held for.
	// Non leaders much wait this long before trying to acquire the lease
	LeaseDuration time.Duration
	// RenewDeadline is the duration that the acting leader will retry refreshing leadership before giving up.
	RenewDeadline time.Duration
	// RetryPeriod is the duration the LeaderElector clients should waite between tries of actions.
	RetryPeriod time.Duration
}
