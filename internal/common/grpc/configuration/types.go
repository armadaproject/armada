package configuration

import "google.golang.org/grpc/keepalive"

type GrpcConfig struct {
	KeepaliveParams            keepalive.ServerParameters
	KeepaliveEnforcementPolicy keepalive.EnforcementPolicy
}
