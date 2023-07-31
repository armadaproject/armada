package configuration

import (
	"time"

	"github.com/armadaproject/armada/internal/armada/configuration"
	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
	"github.com/armadaproject/armada/pkg/client"
)

type PostgresConfig struct {
	PoolMaxOpenConns    int
	PoolMaxIdleConns    int
	PoolMaxConnLifetime time.Duration
	Connection          map[string]string
}

type JobServiceConfiguration struct {
	HttpPort    uint16
	GrpcPort    uint16
	MetricsPort uint16

	Grpc     grpcconfig.GrpcConfig
	GrpcPool grpcconfig.GrpcPoolConfig
	// Connection details that we obtain from client
	ApiConnection client.ApiConnectionDetails
	// Configurable value that translates to number of seconds
	// until a job set subscription is considered expired.
	SubscriptionExpirySecs int64
	// Size of the goroutine pool for processing job-set subscriptions
	SubscriberPoolSize int
	// Purge jobSets if not updated in this number of seconds
	PurgeJobSetTime int64
	// Type of database used - must be either 'postgres' or 'sqlite'
	DatabaseType string
	// Absolute or relative path for sqlite database and must include the db name
	// This field is only read when DatabaseType is 'sqlite'
	DatabasePath string

	// Configuration details for using a Postgres database; this field is
	// ignored if the DatabaseType above is not 'postgres'
	PostgresConfig PostgresConfig

	// Configuration details for using a SQL database
	// only Postgres is supported at the moment
	DatabaseConfig configuration.DatabaseConfig
}
