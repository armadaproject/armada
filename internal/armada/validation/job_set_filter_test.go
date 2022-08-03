package validation

import (
	"testing"

	"github.com/G-Research/armada/pkg/api"
	"github.com/stretchr/testify/assert"
)

func TestValidateJobSetFilter(t *testing.T) {
	result := ValidateJobSetFilter(&api.JobSetFilter{State: []string{"Queued"}})
	assert.NoError(t, result)
}

func TestValidateJobSetFilter_HandlesEmpty(t *testing.T) {
	result := ValidateJobSetFilter(&api.JobSetFilter{})
	assert.NoError(t, result)

	result = ValidateJobSetFilter(&api.JobSetFilter{State: []string{}})
	assert.NoError(t, result)
}

func TestValidateJobSetFilter_ErrorsOnInvalidState(t *testing.T) {
	result := ValidateJobSetFilter(&api.JobSetFilter{State: []string{"Invalid"}})
	assert.Error(t, result)
}

func TestValidateJobSetFilter_EnforcesPendingAndRunningOccurTogether(t *testing.T) {
	result := ValidateJobSetFilter(&api.JobSetFilter{State: []string{"Pending"}})
	assert.Error(t, result)

	result = ValidateJobSetFilter(&api.JobSetFilter{State: []string{"Running"}})
	assert.Error(t, result)

	result = ValidateJobSetFilter(&api.JobSetFilter{State: []string{"Pending", "Running"}})
	assert.NoError(t, result)
}
