package config

import (
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
)

func validatePulsarConfig(t *testing.T, c PulsarConfig) error {
	t.Helper()
	validate := validator.New()
	return validate.Struct(c)
}

func TestPulsarConfig_Validate(t *testing.T) {
	t.Run("DeadLetterMaxAttempts unset is valid", func(t *testing.T) {
		assert.NoError(t, validatePulsarConfig(t, PulsarConfig{}))
	})

	t.Run("DeadLetterMaxAttempts of 1 is invalid", func(t *testing.T) {
		assert.Error(t, validatePulsarConfig(t, PulsarConfig{DeadLetterMaxAttempts: 1}))
	})

	t.Run("DeadLetterMaxAttempts of 2 is valid", func(t *testing.T) {
		assert.NoError(t, validatePulsarConfig(t, PulsarConfig{DeadLetterMaxAttempts: 2}))
	})

	t.Run("MaxBackoffTime below BackoffTime is invalid", func(t *testing.T) {
		assert.Error(t, PulsarConfig{
			BackoffTime:    time.Second,
			MaxBackoffTime: time.Millisecond,
		}.Validate())
	})

	t.Run("MaxBackoffTime at or above BackoffTime is valid", func(t *testing.T) {
		assert.NoError(t, PulsarConfig{
			BackoffTime:    time.Second,
			MaxBackoffTime: time.Second,
		}.Validate())
	})

	t.Run("MaxBackoffTime unset is valid", func(t *testing.T) {
		assert.NoError(t, PulsarConfig{
			BackoffTime: time.Second,
		}.Validate())
	})
}
