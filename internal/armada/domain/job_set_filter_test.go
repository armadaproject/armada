package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsValidFilterState(t *testing.T) {
	assert.True(t, IsValidFilterState("Queued"))
	assert.True(t, IsValidFilterState("Pending"))
	assert.True(t, IsValidFilterState("Running"))
	assert.False(t, IsValidFilterState("Invalid"))
}
