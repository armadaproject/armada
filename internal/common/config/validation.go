package config

import (
	"strings"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
)

func LogValidationErrors(err error) {
	if err != nil {
		for _, err := range err.(validator.ValidationErrors) {
			fieldName := stripPrefix(err.Namespace())
			tag := err.Tag()
			switch tag {
			case "required":
				log.Errorf("ConfigError: Field %s is required but was not found", fieldName)
			default:
				log.Errorf("ConfigError: Field %s has invalid value %s: %s", fieldName, err.Value(), tag)
			}
		}
	}
}

func stripPrefix(s string) string {
	if idx := strings.Index(s, "."); idx != -1 {
		return s[idx+1:]
	}
	return s
}
