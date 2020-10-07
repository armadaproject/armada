package configuration

type NatsConfig struct {
	Servers    []string
	ClusterID  string
	Subject    string
	QueueGroup string
}

type LookoutConfiguration struct {
	Nats                     NatsConfig
	PostgresConnectionString string
}
