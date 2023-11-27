package jobdb

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/types"
)

func TestJobPriorityComparer(t *testing.T) {
	tests := map[string]struct {
		a        *Job
		b        *Job
		expected int
	}{
		"Jobs with equal id are considered equal": {
			a:        &Job{id: "a"},
			b:        &Job{id: "a"},
			expected: 0,
		},
		"Jobs with equal id are considered equal even if priority differs": {
			a:        &Job{id: "a", priority: 1},
			b:        &Job{id: "a", priority: 2},
			expected: 0,
		},
		"Queued jobs are ordered first by increasing priority class priority": {
			a:        &Job{id: "a", priority: 1, priorityClass: types.PriorityClass{Priority: 1}},
			b:        &Job{id: "b", priority: 2, priorityClass: types.PriorityClass{Priority: 2}},
			expected: 1,
		},
		"Queued jobs are ordered second by decreasing priority": {
			a:        &Job{id: "a", priority: 2, priorityClass: types.PriorityClass{Priority: 1}},
			b:        &Job{id: "b", priority: 1, priorityClass: types.PriorityClass{Priority: 1}},
			expected: 1,
		},
		"Queued jobs are ordered third by decreasing submit time": {
			a:        &Job{id: "a", priority: 1, priorityClass: types.PriorityClass{Priority: 1}, submittedTime: 2},
			b:        &Job{id: "b", priority: 1, priorityClass: types.PriorityClass{Priority: 1}, submittedTime: 1},
			expected: 1,
		},
		"Queued jobs are not ordered by runtime": {
			a:        &Job{id: "a", priority: 1, priorityClass: types.PriorityClass{Priority: 1}, activeRunTimestamp: 1, submittedTime: 2},
			b:        &Job{id: "b", priority: 1, priorityClass: types.PriorityClass{Priority: 1}, activeRunTimestamp: 2, submittedTime: 1},
			expected: 1,
		},
		"Queued jobs are ordered fourth by increasing id": {
			a:        &Job{id: "a", priority: 1, priorityClass: types.PriorityClass{Priority: 1}, submittedTime: 1},
			b:        &Job{id: "b", priority: 1, priorityClass: types.PriorityClass{Priority: 1}, submittedTime: 1},
			expected: -1,
		},
		"Running jobs come before queued jobs": {
			a:        &Job{id: "a", priority: 1},
			b:        (&Job{id: "b", priority: 2}).WithNewRun("", "", ""),
			expected: 1,
		},
		"Running jobs are ordered third by runtime": {
			a: (&Job{id: "a", priority: 1, priorityClass: types.PriorityClass{Priority: 1}, submittedTime: 1}).WithUpdatedRun(
				&JobRun{created: 1},
			),
			b: (&Job{id: "b", priority: 1, priorityClass: types.PriorityClass{Priority: 1}, submittedTime: 2}).WithUpdatedRun(
				&JobRun{created: 0},
			),
			expected: 1,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, JobPriorityComparer{}.Compare(tc.a, tc.b))
		})
	}
}
