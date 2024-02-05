package queryapi

import (
	postgresconfig "github.com/armadaproject/armada/internal/armada/configuration"
	authconfig "github.com/armadaproject/armada/internal/common/auth/configuration"
	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
)

type Configuration struct {
	Grpc     grpcconfig.GrpcConfig
	Auth     authconfig.AuthConfig
	Postgres postgresconfig.PostgresConfig
}
