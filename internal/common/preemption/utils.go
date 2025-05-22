package preemption

import (
	"strconv"

	"github.com/armadaproject/armada/internal/server/configuration"
)

// AreRetriesEnabled determines whether preemption retries are enabled at the job level. Also returns whether the
// annotation was set.
func AreRetriesEnabled(annotations map[string]string) (enabled bool, annotationSet bool) {
	preemptionRetryEnabledStr, exists := annotations[configuration.PreemptionRetryEnabledAnnotation]
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
	var preemptionRetryCountMax uint = 0
	preemptionRetryCountMaxStr, exists := annotations[configuration.PreemptionRetryCountMaxAnnotation]

	if !exists {
		return preemptionRetryCountMax, false
	}
	maybePreemptionRetryCountMax, err := strconv.Atoi(preemptionRetryCountMaxStr)
	if err != nil {
		return preemptionRetryCountMax, true
	} else {
		preemptionRetryCountMax = uint(maybePreemptionRetryCountMax)
		return preemptionRetryCountMax, true
	}
}
