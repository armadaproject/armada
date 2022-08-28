package configuration

import (
	"time"

	"github.com/go-redis/redis"

	"github.com/G-Research/armada/internal/armada/configuration"
)

type EventIngesterConfiguration struct {
	// Database configuration
	Redis redis.UniversalOptions
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
	// Pulsar Topic on which sequence updates will be sent
	UpdateTopic string
	// Time after which events will be deleted from the db
	EventRetentionPolicy EventRetentionPolicy
}

type EventRetentionPolicy struct {
	ExpiryEnabled     bool
	RetentionDuration time.Duration
}
