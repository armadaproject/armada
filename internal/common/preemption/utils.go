package preemption

import (
	"strconv"

	"github.com/armadaproject/armada/internal/common/constants"
)

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
	maxRetryCountStr, exists := annotations[constants.PreemptionRetryCountMaxAnnotation]
	if !exists {
		return 0, false
	}
	parsedCount, err := strconv.Atoi(maxRetryCountStr)
	if err != nil {
		return 0, true
	}
	return uint(parsedCount), true
}
