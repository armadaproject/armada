package schedulerobjects

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCalculatePodRequirementsHash(t *testing.T) {
	podReqs := &schedulerobjects.PodRequirements{
		Priority: 1,
	}
	expectedHash, err := protoutil.Hash(podReqs)
	assert.NoError(t, err)

	result, err := CalculatePodRequirementsHash(podReqs)
	assert.NoError(t, err)
	assert.Equal(t, expectedHash, result)

	podReqs.Annotations = map[string]string{"test": "value"}
	result, err = CalculatePodRequirementsHash(podReqs)
	assert.NoError(t, err)
	// The hash should be calculated ignoring annotations and therefore same as the original objects hash
	assert.Equal(t, expectedHash, result)
}

func TestCalculatePodRequirementsHash_NilInput(t *testing.T) {
	result, err := CalculatePodRequirementsHash(nil)

	assert.Error(t, err)
	assert.Equal(t, []byte{}, result)
}
