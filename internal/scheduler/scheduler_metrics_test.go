package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestAggregateJobs(t *testing.T) {
	testJobs := []*jobdb.Job{
		testfixtures.Test1Cpu4GiJob("queue_a", testfixtures.PriorityClass0),
		testfixtures.Test1Cpu4GiJob("queue_b", testfixtures.PriorityClass0),
		testfixtures.Test1Cpu4GiJob("queue_a", testfixtures.PriorityClass0),
		testfixtures.Test1Cpu4GiJob("queue_a", testfixtures.PriorityClass1),
		testfixtures.Test1Cpu4GiJob("queue_a", testfixtures.PriorityClass0),
		testfixtures.Test1Cpu4GiJob("queue_b", testfixtures.PriorityClass1),
		testfixtures.Test1Cpu4GiJob("queue_a", testfixtures.PriorityClass0),
	}

	actual := aggregateJobContexts(map[queuePriorityClassKey]int{}, schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, testJobs))

	expected := map[queuePriorityClassKey]int{
		{queue: "queue_a", priorityClass: testfixtures.PriorityClass0}: 4,
		{queue: "queue_a", priorityClass: testfixtures.PriorityClass1}: 1,
		{queue: "queue_b", priorityClass: testfixtures.PriorityClass0}: 1,
		{queue: "queue_b", priorityClass: testfixtures.PriorityClass1}: 1,
	}

	assert.Equal(t, expected, actual)
}

func TestCalculateFairnessError(t *testing.T) {
	tests := map[string]struct {
		input    map[queuePoolKey]queuePoolData
		expected map[string]float64
	}{
		"empty": {
			input:    map[queuePoolKey]queuePoolData{},
			expected: map[string]float64{},
		},
		"one pool": {
			input: map[queuePoolKey]queuePoolData{
				{pool: "poolA", queue: "queueA"}: {actualShare: 0.5, adjustedFairShare: 0.6},
			},
			expected: map[string]float64{
				"poolA": 0.1,
			},
		},
		"one pool multiple values": {
			input: map[queuePoolKey]queuePoolData{
				{pool: "poolA", queue: "queueA"}: {actualShare: 0.5, adjustedFairShare: 0.6},
				{pool: "poolA", queue: "queueB"}: {actualShare: 0.1, adjustedFairShare: 0.3},
			},
			expected: map[string]float64{
				"poolA": 0.3,
			},
		},
		"one pool one value above fair sahre": {
			input: map[queuePoolKey]queuePoolData{
				{pool: "poolA", queue: "queueA"}: {actualShare: 0.5, adjustedFairShare: 0.6},
				{pool: "poolA", queue: "queueB"}: {actualShare: 0.3, adjustedFairShare: 0.1},
			},
			expected: map[string]float64{
				"poolA": 0.1,
			},
		},
		"two pools": {
			input: map[queuePoolKey]queuePoolData{
				{pool: "poolA", queue: "queueA"}: {actualShare: 0.5, adjustedFairShare: 0.6},
				{pool: "poolB", queue: "queueB"}: {actualShare: 0.1, adjustedFairShare: 0.6},
			},
			expected: map[string]float64{
				"poolA": 0.1,
				"poolB": 0.5,
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			fairnessErrors := calculateFairnessError(tc.input)
			require.Equal(t, len(tc.expected), len(fairnessErrors))
			for pool, err := range tc.expected {
				assert.InDelta(t, err, fairnessErrors[pool], 0.0001, "error for pool %s", pool)
			}
		})
	}
}
