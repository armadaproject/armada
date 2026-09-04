package config

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
)

type formatTestStruct struct {
	Field string `validate:"required"`
}

func TestFormatValidationErrors(t *testing.T) {
	newValidationErrors := func() error { return validator.New().Struct(formatTestStruct{}) }
	assert.EqualError(t, FormatValidationErrors(newValidationErrors()),
		"ConfigError: Field Field is required but was not found")
	assert.EqualError(t, FormatValidationErrors(fmt.Errorf("invalid config: %w", newValidationErrors())),
		"ConfigError: Field Field is required but was not found")
	assert.EqualError(t, FormatValidationErrors(errors.New("some other error")), "some other error")
	assert.EqualError(t, FormatValidationErrors(errors.Join(errors.New("a"), errors.New("b"))), "a\nb")
	assert.Nil(t, FormatValidationErrors(nil))
}
