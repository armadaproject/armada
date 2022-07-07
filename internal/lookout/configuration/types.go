package configuration

import (
	"time"

	"github.com/G-Research/armada/internal/armada/configuration"
	grpcconfig "github.com/G-Research/armada/internal/common/grpc/configuration"
)

type NatsConfig struct {
	Servers   []string
	ClusterID string
	Subject   string
}

type LookoutUIConfig struct {
	ArmadaApiBaseUrl         string
	UserAnnotationPrefix     string
	BinocularsEnabled        bool
	BinocularsBaseUrlPattern string

	OverviewAutoRefreshMs int
	JobSetsAutoRefreshMs  int
	JobsAutoRefreshMs     int
}

type PrunerConfig struct {
	DaysToKeep int
	BatchSize  int
}

type LookoutConfiguration struct {
	HttpPort    uint16
	GrpcPort    uint16
	MetricsPort uint16

	Grpc grpcconfig.GrpcConfig

	UIConfig LookoutUIConfig

	EventQueue             string
	Nats                   NatsConfig
	Jetstream              configuration.JetstreamConfig
	Postgres               configuration.PostgresConfig
	PrunerConfig           PrunerConfig
	DisableEventProcessing bool
}

type LookoutIngesterConfiguration struct {
	// Database configuration
	Postgres configuration.PostgresConfig
	// General Pulsar configuration
	Pulsar configuration.PulsarConfig
	// Pulsar subscription name
	SubscriptionName string
	// Size in bytes above which job specs will be compressed when inserting in the database
	MinJobSpecCompressionSize int
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
	// User annotations have a common prefix to avoid clashes with other annotations.  This prefix will be stripped from
	// The annotation before storing in the db
	UserAnnotationPrefix string
}
