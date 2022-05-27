package configuration

import (
	"time"

	"github.com/G-Research/armada/internal/armada/configuration"
)

type EventIngesterConfiguration struct {

	// Database configuration
	Postgres configuration.PostgresConfig
	// General Pulsar configuration
	Pulsar configuration.PulsarConfig
	// Pulsar subscription name
	SubscriptionName string
	// Size in bytes above which event message will be compressed when inserting in the database
	MinMessageCompressionSize int
	// Number of messages that will be batched together before being inserted into the database
	BatchSize int
	// Maximum time since the last batch before a batch will be inserted into the database
	BatchDuration time.Duration
	// Time for which the pulsar consumer will wait for a new message before retrying
	PulsarReceiveTimeout time.Duration
	// Time for which the pulsar consumer will back off after receiving an error on trying to receive a message
	PulsarBackoffTime time.Duration
	// Number of goroutines to be used for receiving messages and converting them to instructions
	Paralellism int
	// Pulsar Topic on which sequence updates will be sent
	UpdateTopic string
}
