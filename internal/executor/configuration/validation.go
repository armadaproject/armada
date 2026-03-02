package configuration

import (
	"github.com/go-playground/validator/v10"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
)

func (c ExecutorConfiguration) Validate() error {
	validate := validator.New()
	return validate.Struct(c)
}

func (c ExecutorConfiguration) Mutate() (commonconfig.Config, error) {
	return c, nil
}
