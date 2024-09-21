package configuration

import (
	"github.com/go-playground/validator/v10"
)

func (c EventIngesterConfiguration) Validate() error {
	validate := validator.New()
	return validate.Struct(c)
}
