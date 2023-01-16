package validation

import (
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
