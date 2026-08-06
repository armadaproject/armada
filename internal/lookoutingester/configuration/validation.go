package configuration

import (
	"errors"

	"github.com/go-playground/validator/v10"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
)

func (c LookoutIngesterConfiguration) Validate() error {
	validate := validator.New()
	return errors.Join(validate.Struct(c), c.Pulsar.Validate())
}

func (c *LookoutIngesterConfiguration) Mutate() (commonconfig.Config, error) {
	c.Observability.ApplyResourceDefaults("lookoutingester")
	return c, nil
}
