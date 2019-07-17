package configuration

type ArmadaConfig struct {
	GrpcPort string
	Redis    RedisConfig
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}
