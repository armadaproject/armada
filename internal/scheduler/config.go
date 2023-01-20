package scheduler

import (
	"time"

	"github.com/armadaproject/armada/internal/armada/configuration"
)

type Configuration struct {
	// Database configuration
	Postgres configuration.PostgresConfig
	// Metrics configuration
	Metrics configuration.MetricsConfig
	// General Pulsar configuration
	Pulsar configuration.PulsarConfig
	// Pulsar subscription name
	SubscriptionName string
	// Maximum time since the last batch before a batch will be inserted into the database
	BatchDuration time.Duration
	// Time for which the pulsar consumer will wait for a new message before retrying
	PulsarReceiveTimeout time.Duration
	// Time for which the pulsar consumer will back off after receiving an error on trying to receive a message
	PulsarBackoffTime time.Duration
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
