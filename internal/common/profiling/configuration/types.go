package configuration

import (
	authconfig "github.com/armadaproject/armada/internal/common/auth/configuration"
)

type ProfilingConfig struct {
	Port uint16
	Auth *authconfig.AuthConfig
}
