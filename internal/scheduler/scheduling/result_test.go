package scheduling

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
)

func TestPoolSchedulingOutcome_Success(t *testing.T) {
	successfulOutcome := NewPoolSchedulingOutcome(PoolSchedulingTerminationReasonCompleted, nil)
	assert.True(t, successfulOutcome.Success())

	failedOutcome := NewPoolSchedulingOutcome(PoolSchedulingTerminationReasonCompleted, fmt.Errorf("failed"))
	assert.False(t, failedOutcome.Success())
}

func TestTerminationReasonFromString_MapsQueueReasons(t *testing.T) {
	tests := map[string]struct {
		reason   string
		expected PoolSchedulingTerminationReason
	}{
		"queue rate limit": {
			reason:   schedulerconstraints.QueueRateLimitExceededUnschedulableReason,
			expected: PoolSchedulingTerminationReasonRateLimit,
		},
		"queue rate limit exceeded by gang": {
			reason:   schedulerconstraints.QueueRateLimitExceededByGangUnschedulableReason,
			expected: PoolSchedulingTerminationReasonRateLimit,
		},
		"queue new job scheduling duration exceeded": {
			reason:   schedulerconstraints.QueueNewJobSchedulingDurationExceededUnschedulableReason,
			expected: PoolSchedulingTerminationReasonSoftTimeout,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, terminationReasonFromString(tc.reason))
		})
	}
}

func TestPoolSchedulingResult_GetDuration(t *testing.T) {
	now := time.Now()
	result := PoolSchedulingResult{StartTime: now, EndTime: now.Add(time.Second * 5)}
	assert.Equal(t, time.Second*5, result.GetDuration())

	noEndTime := PoolSchedulingResult{StartTime: now}
	assert.True(t, noEndTime.GetDuration() == 0)

	noStartTime := PoolSchedulingResult{EndTime: now.Add(time.Second * 5)}
	assert.True(t, noStartTime.GetDuration() == 0)

	noTime := PoolSchedulingResult{}
	assert.True(t, noTime.GetDuration() == 0)
}
