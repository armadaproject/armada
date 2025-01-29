package prioritymultiplier

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNoOp(t *testing.T) {
	testCases := []multiplierKey{
		{
			pool:  "testPool",
			queue: "testQueue",
		},
		{
			pool:  "",
			queue: "",
		},
		{
			pool:  "testPool2",
			queue: "testQueue2",
		},
	}

	provider := NewNoOpProvider()
	for _, testCase := range testCases {
		testName := fmt.Sprintf("%s_%s", testCase.pool, testCase.queue)
		assert.True(t, provider.Ready(), testName)
		multiplier, err := provider.Multiplier(testCase.pool, testCase.queue)
		assert.NoError(t, err, testName)
		assert.Equal(t, 1.0, multiplier, testName)
	}

}
