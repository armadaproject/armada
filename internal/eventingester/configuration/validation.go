package configuration

import (
	"fmt"

	"github.com/go-playground/validator/v10"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
)

func (c EventIngesterConfiguration) Validate() error {
	validate := validator.New()
	if err := validate.Struct(c); err != nil {
		return err
	}

	if c.Metrics.Redis.Enabled && c.Metrics.Redis.ConnectionInfo.MasterName != "" {
		if len(c.Metrics.Redis.ConnectionInfo.Addrs) == 0 {
			return fmt.Errorf("metrics.redis.connectionInfo.addrs is required when sentinel mode is enabled (masterName is set)")
		}

		if !c.Metrics.Redis.ConnectionInfo.ReadOnly {
			return fmt.Errorf("metrics.redis.connectionInfo.readOnly must be true when sentinel mode is enabled (masterName is set) to ensure replica-only routing")
		}

	}

	return nil
}

func (c EventIngesterConfiguration) Mutate() (commonconfig.Config, error) {
	return c, nil
}
