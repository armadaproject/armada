package configuration

import (
	"os"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
	"github.com/armadaproject/armada/internal/common/observability"
	"github.com/armadaproject/armada/internal/lookout/version"
)

func (c ExecutorConfiguration) Validate() error {
	validate := validator.New()
	return validate.Struct(c)
}

func (c *ExecutorConfiguration) Mutate() (commonconfig.Config, error) {
	serviceInstance, err := os.Hostname()
	if err != nil {
		serviceInstance = uuid.New().String()
	}
	observabilityConfig, err := c.Observability.WithDefaults(observability.ResourceAttributes{
		ServiceName:     "executor",
		ServiceVersion:  version.Version,
		ServiceInstance: serviceInstance,
	})
	if err != nil {
		return nil, err
	}
	c.Observability = observabilityConfig
	return c, nil
}
