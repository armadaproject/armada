package configuration

import (
	"github.com/go-playground/validator/v10"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
)

func (c BinocularsConfig) Validate() error {
	validate := validator.New()
	return validate.Struct(c)
}

func (c BinocularsConfig) Mutate() (commonconfig.Config, error) {
	return c, nil
}
