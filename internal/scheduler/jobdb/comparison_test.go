package jobdb

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/scheduler/pricing"
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
			b:        (&Job{id: "b", priority: 2, jobDb: NewJobDb(map[string]types.PriorityClass{"foo": {}}, "foo", stringinterner.New(1), testResourceListFactory)}).WithNewRun("", "", "", "", 0),
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

func TestMarketJobPriorityComparer(t *testing.T) {
	bidPricesA := map[string]pricing.Bid{"a": {QueuedBid: 1, RunningBid: 1}, "b": {QueuedBid: 2, RunningBid: 2}}
	bidPricesB := map[string]pricing.Bid{"a": {QueuedBid: 2, RunningBid: 2}, "b": {QueuedBid: 1, RunningBid: 1}}
	tests := map[string]struct {
		a           *Job
		b           *Job
		currentPool string
		expected    int
	}{
		"Jobs with equal id are considered equal": {
			a:           &Job{id: "a"},
			b:           &Job{id: "a"},
			currentPool: "a",
			expected:    0,
		},
		"Queued jobs are ordered first by increasing priority class priority": {
			a:           &Job{id: "a", queued: true, priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}},
			b:           &Job{id: "b", queued: true, priorityClass: types.PriorityClass{Priority: 2, Preemptible: true}},
			currentPool: "a",
			expected:    1,
		},
		"Queued jobs are ordered second by decreasing bid price for current pool": {
			a:           &Job{id: "a", queued: true, priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesA},
			b:           &Job{id: "b", queued: true, priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesB},
			currentPool: "a",
			expected:    1,
		},
		"Queued jobs are ordered second by decreasing bid price for current pool - pools reversed": {
			a:           &Job{id: "a", queued: true, priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesA},
			b:           &Job{id: "b", queued: true, priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesB},
			currentPool: "b",
			expected:    -1,
		},
		"Queued jobs are ordered fourth by decreasing submit time": {
			a:           &Job{id: "a", queued: true, priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesA, submittedTime: 2},
			b:           &Job{id: "b", queued: true, priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesA, submittedTime: 1},
			currentPool: "a",
			expected:    1,
		},
		"Queued jobs are not ordered by runtime": {
			a:           &Job{id: "a", queued: true, priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesA, activeRunTimestamp: 1, submittedTime: 2},
			b:           &Job{id: "b", queued: true, priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesA, activeRunTimestamp: 2, submittedTime: 1},
			currentPool: "a",
			expected:    1,
		},
		"Queued jobs are ordered fifth by increasing id": {
			a:           &Job{id: "a", queued: true, priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesA, submittedTime: 1},
			b:           &Job{id: "b", queued: true, priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesA, submittedTime: 1},
			currentPool: "a",
			expected:    -1,
		},
		"Queued and running jobs are first ordered on priority class": {
			a: &Job{id: "a", priorityClass: types.PriorityClass{Priority: 2, Preemptible: true}},
			b: (&Job{
				id: "b", priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesB,
				jobDb: NewJobDb(map[string]types.PriorityClass{"foo": {}}, "foo", stringinterner.New(1), testResourceListFactory),
			}).WithNewRun("", "", "", "", 0),
			currentPool: "a",
			expected:    -1,
		},
		"Queued and running jobs are second ordered on bid price": {
			a: &Job{id: "a", priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesA},
			b: (&Job{
				id: "b", priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesB,
				jobDb: NewJobDb(map[string]types.PriorityClass{"foo": {}}, "foo", stringinterner.New(1), testResourceListFactory),
			}).WithNewRun("", "", "", "", 0),
			currentPool: "b",
			expected:    -1,
		},
		"Running jobs are ordered over queued jobs if priority class and bid price are equal": {
			a: &Job{id: "a", priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesA},
			b: (&Job{
				id: "b", priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesA,
				jobDb: NewJobDb(map[string]types.PriorityClass{"foo": {}}, "foo", stringinterner.New(1), testResourceListFactory),
			}).WithNewRun("", "", "", "", 0),
			currentPool: "a",
			expected:    1,
		},
		"Running jobs are ordered third by runtime": {
			a: (&Job{id: "a", priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesA, submittedTime: 1}).WithUpdatedRun(
				&JobRun{created: 1},
			),
			b: (&Job{id: "b", priorityClass: types.PriorityClass{Priority: 1, Preemptible: true}, bidPricesPool: bidPricesA, submittedTime: 2}).WithUpdatedRun(
				&JobRun{created: 0},
			),
			currentPool: "a",
			expected:    1,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, MarketJobPriorityComparer{Pool: tc.currentPool}.Compare(tc.a, tc.b))
		})
	}
}
