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

	var parsedValidationErrors validator.ValidationErrors
	if !errors.As(err, &parsedValidationErrors) {
		var invalidValidationError *validator.InvalidValidationError
		if errors.As(err, &invalidValidationError) {
			return fmt.Errorf("ConfigError: invalid validation error: %w", invalidValidationError)
		}

		return err
	}

	if len(parsedValidationErrors) == 0 {
		return err
	}

	var validationErrors error
	for _, validationError := range parsedValidationErrors {
		fieldName := stripPrefix(validationError.Namespace())
		tag := validationError.Tag()
		switch tag {
		case "required":
			validationErrors = errors.Join(validationErrors, fmt.Errorf("ConfigError: Field %s is required but was not found", fieldName))
		default:
			validationErrors = errors.Join(validationErrors, fmt.Errorf("ConfigError: Field %s has invalid value %s: %s", fieldName, validationError.Value(), tag))
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
