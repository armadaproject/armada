package preemption

import (
	"github.com/armadaproject/armada/internal/server/configuration"
	"strconv"
)

// AreRetriesEnabled determines whether preemption retries are enabled at the job level. Also returns whether the
// annotation was set.
func AreRetriesEnabled(annotations map[string]string) (bool, bool) {
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
func GetMaxRetryCount(annotations map[string]string) (uint, bool) {
	var preemptionRetryCountMax uint = 0
	preemptionRetryCountMaxStr, exists := annotations[configuration.PreemptionRetryCountMaxAnnotation]

	if !exists {
		return 0, false
	}
	maybePreemptionRetryCountMax, err := strconv.Atoi(preemptionRetryCountMaxStr)
	if err != nil {
		return 0, true
	} else {
		preemptionRetryCountMax = uint(maybePreemptionRetryCountMax)
	}

	return preemptionRetryCountMax, true
}
