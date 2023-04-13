package configuration

import (
	"time"

	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
	"github.com/armadaproject/armada/pkg/client"
)

type PostgresConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	Connection      map[string]string
}

type JobServiceConfiguration struct {
	HttpPort    uint16
	GrpcPort    uint16
	MetricsPort uint16

	Grpc grpcconfig.GrpcConfig
	// Connection details that we obtain from client
	ApiConnection client.ApiConnectionDetails
	// Configurable value that translates to number of seconds
	// This is the amount of time since the last job in job-set has been updated.
	SubscribeJobSetTime int64
	// Purging JobSets
	PurgeJobSetTime int64
	// Type of database used - must be either 'postgres' or 'sqlite'
	DatabaseType string
	// Absolute or relative path for sqlite database and must include the db name
	// This field is only read when DatabaseType is 'sqlite'
	DatabasePath string

	// Configuration details for using a Postgres database; this field is
	// ignored if the DatabaseType above is not 'postgres'
	PostgresConfig PostgresConfig
}
