package configuration

import (
	"errors"

	"github.com/go-playground/validator/v10"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
)

func (c EventIngesterConfiguration) Validate() error {
	validate := validator.New()
	return errors.Join(validate.Struct(c), c.Pulsar.Validate())
}

func (c *EventIngesterConfiguration) Mutate() (commonconfig.Config, error) {
	c.Observability.ApplyResourceDefaults("eventingester")
	return c, nil
}
