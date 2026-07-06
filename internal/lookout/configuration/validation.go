package configuration

import (
	"github.com/go-playground/validator/v10"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
)

func (c LookoutConfig) Validate() error {
	validate := validator.New()
	return validate.Struct(c)
}

func (c *LookoutConfig) Mutate() (commonconfig.Config, error) {
	c.Observability.ApplyResourceDefaults("lookout")
	return c, nil
}
