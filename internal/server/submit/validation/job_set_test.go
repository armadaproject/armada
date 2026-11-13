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
		"reason with NUL byte": {
			reason:      "a\x00b",
			expectError: true,
		},
		"reason with invalid UTF-8": {
			reason:      "a\xffb",
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

func TestValidateStorableText(t *testing.T) {
	tests := map[string]struct {
		value       string
		expectError bool
	}{
		"empty":                {value: "", expectError: false},
		"ascii":                {value: "job-set-1", expectError: false},
		"multi-byte UTF-8":     {value: "jöb-sét-ü", expectError: false},
		"NUL byte":             {value: "a\x00b", expectError: true},
		"invalid UTF-8":        {value: "a\xffb", expectError: true},
		"truncated UTF-8 rune": {value: "a\xe2\x82", expectError: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidateStorableText("Field", tc.value)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateQueueAndJobSet(t *testing.T) {
	tests := map[string]struct {
		queue       string
		jobSetId    string
		expectError bool
	}{
		"valid":                      {queue: "queue", jobSetId: "job-set", expectError: false},
		"empty queue":                {queue: "", jobSetId: "job-set", expectError: true},
		"empty job set":              {queue: "queue", jobSetId: "", expectError: true},
		"job set with NUL byte":      {queue: "queue", jobSetId: "a\x00b", expectError: true},
		"job set with invalid UTF-8": {queue: "queue", jobSetId: "a\xffb", expectError: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidateQueueAndJobSet(&api.JobCancelRequest{Queue: tc.queue, JobSetId: tc.jobSetId})
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
