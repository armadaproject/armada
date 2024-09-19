package configuration

import (
	"google.golang.org/grpc/keepalive"
)

type GrpcConfig struct {
	Port                       int
	KeepaliveParams            keepalive.ServerParameters
	KeepaliveEnforcementPolicy keepalive.EnforcementPolicy
	Tls                        TlsConfig
}

type TlsConfig struct {
	Enabled  bool
	KeyPath  string
	CertPath string
}
