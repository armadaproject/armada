package configuration

import (
	"github.com/go-playground/validator/v10"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
)

func (c LookoutIngesterConfiguration) Validate() error {
	validate := validator.New()
	return validate.Struct(c)
}

func (c LookoutIngesterConfiguration) Mutate() (commonconfig.Config, error) {
	return c, nil
}
