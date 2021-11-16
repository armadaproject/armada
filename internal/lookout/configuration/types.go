package configuration

import (
	"time"

	"github.com/G-Research/armada/internal/armada/configuration"
)

type NatsConfig struct {
	Servers    []string
	ClusterID  string
	Subject    string
	QueueGroup string
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

type PostgresConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	Connection      map[string]string
}

type LookoutConfiguration struct {
	HttpPort    uint16
	GrpcPort    uint16
	MetricsPort uint16

	UIConfig LookoutUIConfig

	Nats            NatsConfig
	JetstreamConfig configuration.JetstreamConfig
	Postgres        PostgresConfig
}
