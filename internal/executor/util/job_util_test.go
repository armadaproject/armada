package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractJobIdFromName(t *testing.T) {
	validInput := "armada-01gd3fet6mf1g9km91ajhg208g-0"
	expected1 := "01gd3fet6mf1g9km91ajhg208g"
	extracted, err := ExtractJobIdFromName(validInput)
	assert.Nil(t, err)
	assert.Equal(t, extracted, expected1)

	invalidInput := "armada-2131fdsf-fesgs3-0"
	extracted, err = ExtractJobIdFromName(invalidInput)
	assert.Empty(t, extracted)
	assert.EqualError(t, err, "invalid name format: expected 'armada-<UID>-0(-<suffix>)', received 'armada-2131fdsf-fesgs3-0'")

	invalidInput = "really-bad-input"
	extracted, err = ExtractJobIdFromName(invalidInput)
	assert.Empty(t, extracted)
	assert.EqualError(t, err, "invalid name format: expected 'armada-<UID>-0(-<suffix>)', received 'really-bad-input'")
}

func TestParsePreemptionMessage(t *testing.T) {
	validInput := "Preempted by some-namespace-1/armada-feuwet4nui43nfekjng-0 on node test-node"
	expected := &PreemptiveJobInfo{
		Namespace: "some-namespace-1",
		Name:      "armada-feuwet4nui43nfekjng-0",
		Node:      "test-node",
	}
	extracted, err := ParsePreemptionMessage(validInput)
	assert.Nil(t, err)
	assert.Equal(t, extracted, expected)

	invalidInput := "Preempted by armada-feuwet4nui43nfekjng-0 on node test-node"
	extracted, err = ParsePreemptionMessage(invalidInput)
	assert.Nil(t, extracted)
	assert.EqualError(
		t,
		err,
		"invalid preemption message: expected 'Preempted by ([-a-zA-Z0-9]+)/([-a-zA-Z0-9]+) on node ([-a-zA-Z0-9]+)', "+
			"received 'Preempted by armada-feuwet4nui43nfekjng-0 on node test-node'",
	)

	invalidInput = "really bad input"
	extracted, err = ParsePreemptionMessage(invalidInput)
	assert.Nil(t, extracted)
	assert.EqualError(
		t,
		err,
		"invalid preemption message: expected 'Preempted by ([-a-zA-Z0-9]+)/([-a-zA-Z0-9]+) on node ([-a-zA-Z0-9]+)', "+
			"received 'really bad input'",
	)
}
