package validation

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/pkg/api"
)

func TestValidateJobSetFilter(t *testing.T) {
	result := ValidateJobSetFilter(&api.JobSetFilter{States: []api.JobState{api.JobState_QUEUED}})
	assert.NoError(t, result)
}

func TestValidateJobSetFilter_HandlesEmpty(t *testing.T) {
	result := ValidateJobSetFilter(&api.JobSetFilter{})
	assert.NoError(t, result)

	result = ValidateJobSetFilter(&api.JobSetFilter{States: []api.JobState{}})
	assert.NoError(t, result)
}

func TestValidateJobSetFilter_EnforcesPendingAndRunningOccurTogether(t *testing.T) {
	result := ValidateJobSetFilter(&api.JobSetFilter{States: []api.JobState{api.JobState_PENDING}})
	assert.Error(t, result)

	result = ValidateJobSetFilter(&api.JobSetFilter{States: []api.JobState{api.JobState_RUNNING}})
	assert.Error(t, result)

	result = ValidateJobSetFilter(&api.JobSetFilter{States: []api.JobState{api.JobState_PENDING, api.JobState_RUNNING}})
	assert.NoError(t, result)
}

func TestValidateReason(t *testing.T) {
	tests := map[string]struct {
		reason      string
		expectError bool
	}{
		"empty reason": {
			reason:      "",
			expectError: false,
		},
		"reason at max length": {
			reason:      strings.Repeat("a", MaxReasonBytes),
			expectError: false,
		},
		"reason over max length": {
			reason:      strings.Repeat("a", MaxReasonBytes+1),
			expectError: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := ValidateReason(&api.JobCancelRequest{Reason: tc.reason})
			if tc.expectError {
				assert.Error(t, result)
			} else {
				assert.NoError(t, result)
			}
		})
	}
}
