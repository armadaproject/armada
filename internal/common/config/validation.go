package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"
)

type Config interface {
	Validate() error
}

func FormatValidationErrors(err error) error {
	var validationErrors error
	for _, err := range err.(validator.ValidationErrors) {
		fieldName := stripPrefix(err.Namespace())
		tag := err.Tag()
		switch tag {
		case "required":
			validationErrors = errors.Join(validationErrors, fmt.Errorf("ConfigError: Field %s is required but was not found", fieldName))
		default:
			validationErrors = errors.Join(validationErrors, fmt.Errorf("ConfigError: Field %s has invalid value %s: %s", fieldName, err.Value(), tag))
		}
	}
	return validationErrors
}

func stripPrefix(s string) string {
	if idx := strings.Index(s, "."); idx != -1 {
		return s[idx+1:]
	}
	return s
}
