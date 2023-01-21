package configuration

import "google.golang.org/grpc/keepalive"

type GrpcConfig struct {
	Port                       int `validate:"required"`
	KeepaliveParams            keepalive.ServerParameters
	KeepaliveEnforcementPolicy keepalive.EnforcementPolicy
}
