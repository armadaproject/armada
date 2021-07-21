package configuration

import "time"

type NatsConfig struct {
	Servers    []string
	ClusterID  string
	Subject    string
	QueueGroup string
}

type LookoutUIConfig struct {
	ArmadaApiBaseUrl         string
	UserAnnotationPrefix     string
	BinocularsBaseUrlPattern string
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

	Nats     NatsConfig
	Postgres PostgresConfig
}
