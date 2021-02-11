package configuration

type NatsConfig struct {
	Servers    []string
	ClusterID  string
	Subject    string
	QueueGroup string
}

type LookoutUIConfig struct {
	ArmadaApiBaseUrl string
}

type LookoutConfiguration struct {
	HttpPort    uint16
	GrpcPort    uint16
	MetricsPort uint16

	UIConfig LookoutUIConfig

	Nats               NatsConfig
	PostgresConnection map[string]string
}
