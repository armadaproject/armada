package scheduling

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPoolSchedulingOutcome_Success(t *testing.T) {
	successfulOutcome := NewPoolSchedulingOutcome(PoolSchedulingTerminationReasonCompleted, nil)
	assert.True(t, successfulOutcome.Success())

	failedOutcome := NewPoolSchedulingOutcome(PoolSchedulingTerminationReasonCompleted, fmt.Errorf("failed"))
	assert.False(t, failedOutcome.Success())
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
