package config

import (
	"strings"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
)

func Validate(config interface{}) error {
	validate := validator.New()
	return validate.Struct(config)
}

func LogValidationErrors(err error) {
	if err != nil {
		for _, err := range err.(validator.ValidationErrors) {
			fieldName := stripPrefix(err.Namespace())
			switch err.Tag() {
			case "required":
				log.Errorf("ConfigError: Field %s is required but was not found", fieldName)
			default:
				log.Errorf("ConfigError: %s is not a valid value for %s", err.Value(), fieldName)
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
