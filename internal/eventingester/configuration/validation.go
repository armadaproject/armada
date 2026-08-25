package configuration

import (
	"fmt"
	"time"

	"github.com/go-playground/validator/v10"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
)

// DefaultRetryInitialBackoff is used when RetryInitialBackoff is unset.
// It lives here so validation can reason about effective values without
// importing the collector package (which depends on this one).
const DefaultRetryInitialBackoff = 500 * time.Millisecond

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

	effectiveInitialBackoff := c.RetryInitialBackoff
	if effectiveInitialBackoff == 0 {
		effectiveInitialBackoff = DefaultRetryInitialBackoff
	}
	if c.RetryMaxBackoff > 0 && effectiveInitialBackoff > c.RetryMaxBackoff {
		if c.RetryInitialBackoff == 0 {
			sl.ReportError(c.RetryMaxBackoff, "RetryMaxBackoff", "", fmt.Sprintf("retryMaxBackoff (%s) is below the default retryInitialBackoff (%s); set retryInitialBackoff explicitly or raise retryMaxBackoff", c.RetryMaxBackoff, DefaultRetryInitialBackoff), "")
		} else {
			sl.ReportError(c.RetryInitialBackoff, "RetryInitialBackoff", "", fmt.Sprintf("retryInitialBackoff (%s) must not exceed retryMaxBackoff (%s)", c.RetryInitialBackoff, c.RetryMaxBackoff), "")
		}
	}
}

func (c *EventIngesterConfiguration) Mutate() (commonconfig.Config, error) {
	c.Observability.ApplyResourceDefaults("eventingester")
	return c, nil
}
