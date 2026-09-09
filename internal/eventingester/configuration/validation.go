package configuration

import (
	"github.com/go-playground/validator/v10"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
)

func (c EventIngesterConfiguration) Validate() error {
	validate := validator.New()
	validate.RegisterStructValidation(redisMemoryMetricsConfigValidation, RedisMemoryMetricsConfig{})
	return validate.Struct(c)
}

func redisMemoryMetricsConfigValidation(sl validator.StructLevel) {
	c := sl.Current().Interface().(RedisMemoryMetricsConfig)

	if c.RetryInitialBackoff < 0 {
		sl.ReportError(c.RetryInitialBackoff, "RetryInitialBackoff", "", "retryInitialBackoff must be non-negative", "")
	}
}

func (c *EventIngesterConfiguration) Mutate() (commonconfig.Config, error) {
	c.Observability.ApplyResourceDefaults("eventingester")
	return c, nil
}
