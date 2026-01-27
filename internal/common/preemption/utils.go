package preemption

import (
	"strconv"

	"github.com/armadaproject/armada/internal/common/constants"
)

// RetryConfig contains preemption retry settings.
// Used by both server (submission validation) and scheduler (runtime behavior).
type RetryConfig struct {
	// Enabled toggles preemption retries globally.
	// When false, jobs with preemption retry annotations are rejected at submission.
	Enabled bool
	// MaxRetryCount is the maximum retry count users can request via annotations.
	// Used by server to validate job submissions. If nil, no upper bound is enforced.
	MaxRetryCount *uint
	// DefaultRetryCount is the retry count applied when jobs don't specify via annotation.
	// Used by scheduler at runtime. If nil, defaults to 0 (no retries unless job specifies).
	DefaultRetryCount *uint
}

// AreRetriesEnabled determines whether preemption retries are enabled at the job level. Also returns whether the
// annotation was set.
func AreRetriesEnabled(annotations map[string]string) (enabled bool, annotationSet bool) {
	preemptionRetryEnabledStr, exists := annotations[constants.PreemptionRetryEnabledAnnotation]
	if !exists {
		return false, false
	}

	preemptionRetryEnabled, err := strconv.ParseBool(preemptionRetryEnabledStr)
	if err != nil {
		return false, true
	}
	return preemptionRetryEnabled, true
}

// GetMaxRetryCount gets the max preemption retry count at a job level. Also returns whether the annotation was set.
func GetMaxRetryCount(annotations map[string]string) (maxRetryCount uint, annotationSet bool) {
	maxRetryCountStr, exists := annotations[constants.PreemptionMaxRetryCountAnnotation]
	if !exists {
		return 0, false
	}
	parsedCount, err := strconv.Atoi(maxRetryCountStr)
	if err != nil {
		return 0, true
	}
	return uint(parsedCount), true
}
