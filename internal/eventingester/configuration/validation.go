package configuration

import (
	"fmt"

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
	if c.RetryMaxBackoff < 0 {
		sl.ReportError(c.RetryMaxBackoff, "RetryMaxBackoff", "", "retryMaxBackoff must be non-negative", "")
	}
	if c.RetryInitialBackoff > 0 && c.RetryMaxBackoff > 0 && c.RetryInitialBackoff > c.RetryMaxBackoff {
		sl.ReportError(c.RetryInitialBackoff, "RetryInitialBackoff", "", fmt.Sprintf("retryInitialBackoff (%s) must not exceed retryMaxBackoff (%s)", c.RetryInitialBackoff, c.RetryMaxBackoff), "")
	}
}

func (c *EventIngesterConfiguration) Mutate() (commonconfig.Config, error) {
	c.Observability.ApplyResourceDefaults("eventingester")
	return c, nil
}
