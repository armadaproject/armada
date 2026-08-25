package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"
)

type Config interface {
	Mutate() (Config, error)
	Validate() error
}

func FormatValidationErrors(err error) error {
	if err == nil {
		return nil
	}
	var ve validator.ValidationErrors
	if !errors.As(err, &ve) {
		return err
	}
	var formatted error
	for _, err := range ve {
		fieldName := stripPrefix(err.Namespace())
		tag := err.Tag()
		switch tag {
		case "required":
			formatted = errors.Join(formatted, fmt.Errorf("ConfigError: Field %s is required but was not found", fieldName))
		default:
			formatted = errors.Join(formatted, fmt.Errorf("ConfigError: Field %s has invalid value %s: %s", fieldName, err.Value(), tag))
		}
	}
	return formatted
}

func stripPrefix(s string) string {
	if idx := strings.Index(s, "."); idx != -1 {
		return s[idx+1:]
	}
	return s
}
