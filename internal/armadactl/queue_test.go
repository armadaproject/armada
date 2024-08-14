package armadactl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/pkg/api"
)

func TestQueueFiltering(t *testing.T) {
	tests := map[string]struct {
		queuesToFilter     []*api.Queue
		args               *QueueQueryArgs
		expectedQueueNames []string
		errorExpected      bool
	}{
		"empty filter": {
			queuesToFilter: []*api.Queue{
				{
					Name: "queue-a",
				},
				{
					Name: "queue-b",
				},
			},
			args:               &QueueQueryArgs{},
			expectedQueueNames: []string{"queue-a", "queue-b"},
			errorExpected:      false,
		},
		"query single queue by name": {
			queuesToFilter: []*api.Queue{
				{
					Name: "queue-a",
				},
				{
					Name: "queue-b",
				},
			},
			args: &QueueQueryArgs{
				InQueueNames: []string{"queue-a"},
			},
			expectedQueueNames: []string{"queue-a"},
			errorExpected:      false,
		},
		"query non-matching queue by name": {
			queuesToFilter: []*api.Queue{
				{
					Name: "queue-a",
				},
				{
					Name: "queue-b",
				},
			},
			args: &QueueQueryArgs{
				InQueueNames: []string{"queue-z"},
			},
			expectedQueueNames: []string{},
			errorExpected:      false,
		},
		"query multiple queues by name": {
			queuesToFilter: []*api.Queue{
				{
					Name: "queue-a",
				},
				{
					Name: "queue-b",
				},
			},
			args: &QueueQueryArgs{
				InQueueNames: []string{"queue-a", "queue-b"},
			},
			expectedQueueNames: []string{"queue-a", "queue-b"},
			errorExpected:      false,
		},
		"filter on single label": {
			queuesToFilter: []*api.Queue{
				{
					Name: "queue-a",
					Labels: map[string]string{
						"armadaproject.io/pool": "cpu",
					},
					Cordoned: false,
				},
				{
					Name: "queue-b",
					Labels: map[string]string{
						"armadaproject.io/pool": "gpu",
					},
					Cordoned: false,
				},
			},
			args: &QueueQueryArgs{
				ContainsAllLabels: []string{
					"armadaproject.io/pool=cpu",
				},
			},
			expectedQueueNames: []string{"queue-a"},
			errorExpected:      false,
		},
		"filter on single label key": {
			queuesToFilter: []*api.Queue{
				{
					Name: "queue-a",
					Labels: map[string]string{
						"armadaproject.io/pool": "cpu",
					},
					Cordoned: false,
				},
				{
					Name: "queue-b",
					Labels: map[string]string{
						"armadaproject.io/pool": "gpu",
					},
					Cordoned: false,
				},
				{
					Name: "queue-c",
					Labels: map[string]string{
						"armadaproject.io/pool": "mixed",
					},
					Cordoned: false,
				},
			},
			args: &QueueQueryArgs{
				ContainsAllLabels: []string{
					"armadaproject.io/pool",
				},
			},
			expectedQueueNames: []string{"queue-a", "queue-b", "queue-c"},
			errorExpected:      false,
		},
		"filter on multiple labels": {
			queuesToFilter: []*api.Queue{
				{
					Name: "queue-a",
					Labels: map[string]string{
						"armadaproject.io/pool":     "cpu",
						"armadaproject.io/priority": "high",
					},
					Cordoned: false,
				},
				{
					Name: "queue-b",
					Labels: map[string]string{
						"armadaproject.io/pool":     "gpu",
						"armadaproject.io/priority": "medium",
					},
					Cordoned: false,
				},
				{
					Name: "queue-c",
					Labels: map[string]string{
						"armadaproject.io/pool":     "mixed",
						"armadaproject.io/priority": "low",
					},
					Cordoned: false,
				},
			},
			args: &QueueQueryArgs{
				ContainsAllLabels: []string{
					"armadaproject.io/pool", "armadaproject.io/priority=high",
				},
			},
			expectedQueueNames: []string{"queue-a"},
			errorExpected:      false,
		},
		"filter on multiple labels and queue name": {
			queuesToFilter: []*api.Queue{
				{
					Name: "queue-a",
					Labels: map[string]string{
						"armadaproject.io/pool":     "cpu",
						"armadaproject.io/priority": "high",
					},
					Cordoned: false,
				},
				{
					Name: "queue-b",
					Labels: map[string]string{
						"armadaproject.io/pool":     "gpu",
						"armadaproject.io/priority": "medium",
					},
					Cordoned: false,
				},
				{
					Name: "queue-c",
					Labels: map[string]string{
						"armadaproject.io/pool":     "mixed",
						"armadaproject.io/priority": "low",
					},
					Cordoned: false,
				},
			},
			args: &QueueQueryArgs{
				ContainsAllLabels: []string{
					"armadaproject.io/pool", "armadaproject.io/priority=high",
				},
				InQueueNames: []string{
					"queue-a",
				},
			},
			expectedQueueNames: []string{"queue-a"},
			errorExpected:      false,
		},
		"filter on multiple labels and queue name, no matches": {
			queuesToFilter: []*api.Queue{
				{
					Name: "queue-a",
					Labels: map[string]string{
						"armadaproject.io/pool":     "cpu",
						"armadaproject.io/priority": "high",
					},
					Cordoned: false,
				},
				{
					Name: "queue-b",
					Labels: map[string]string{
						"armadaproject.io/pool":     "gpu",
						"armadaproject.io/priority": "medium",
					},
					Cordoned: false,
				},
				{
					Name: "queue-c",
					Labels: map[string]string{
						"armadaproject.io/pool":     "mixed",
						"armadaproject.io/priority": "low",
					},
					Cordoned: false,
				},
			},
			args: &QueueQueryArgs{
				ContainsAllLabels: []string{
					"armadaproject.io/pool", "armadaproject.io/priority=high",
				},
				InQueueNames: []string{
					"queue-b",
				},
			},
			expectedQueueNames: []string{},
			errorExpected:      false,
		},
		"filter on cordoned status": {
			queuesToFilter: []*api.Queue{
				{
					Name: "queue-a",
					Labels: map[string]string{
						"armadaproject.io/pool":     "cpu",
						"armadaproject.io/priority": "high",
					},
					Cordoned: false,
				},
				{
					Name: "queue-b",
					Labels: map[string]string{
						"armadaproject.io/pool":     "gpu",
						"armadaproject.io/priority": "medium",
					},
					Cordoned: true,
				},
				{
					Name: "queue-c",
					Labels: map[string]string{
						"armadaproject.io/pool":     "mixed",
						"armadaproject.io/priority": "low",
					},
					Cordoned: false,
				},
			},
			args: &QueueQueryArgs{
				OnlyCordoned: true,
			},
			expectedQueueNames: []string{"queue-b"},
			errorExpected:      false,
		},
		"filter on label and cordoned status": {
			queuesToFilter: []*api.Queue{
				{
					Name: "queue-a",
					Labels: map[string]string{
						"armadaproject.io/pool":     "cpu",
						"armadaproject.io/priority": "high",
					},
					Cordoned: false,
				},
				{
					Name: "queue-b",
					Labels: map[string]string{
						"armadaproject.io/pool":     "gpu",
						"armadaproject.io/priority": "medium",
					},
					Cordoned: false,
				},
				{
					Name: "queue-c",
					Labels: map[string]string{
						"armadaproject.io/pool":     "mixed",
						"armadaproject.io/priority": "low",
					},
					Cordoned: true,
				},
			},
			args: &QueueQueryArgs{
				ContainsAllLabels: []string{
					"armadaproject.io/pool=mixed",
				},
				OnlyCordoned: true,
			},
			expectedQueueNames: []string{"queue-c"},
			errorExpected:      false,
		},
		"simple query inverted": {
			queuesToFilter: []*api.Queue{
				{
					Name: "queue-a",
					Labels: map[string]string{
						"armadaproject.io/pool":     "cpu",
						"armadaproject.io/priority": "high",
					},
					Cordoned: false,
				},
				{
					Name: "queue-b",
					Labels: map[string]string{
						"armadaproject.io/pool":     "gpu",
						"armadaproject.io/priority": "medium",
					},
					Cordoned: false,
				},
				{
					Name: "queue-c",
					Labels: map[string]string{
						"armadaproject.io/pool":     "mixed",
						"armadaproject.io/priority": "low",
					},
					Cordoned: true,
				},
			},
			args: &QueueQueryArgs{
				InQueueNames: []string{"queue-a"},
				InvertResult: true,
			},
			expectedQueueNames: []string{"queue-b", "queue-c"},
			errorExpected:      false,
		},
		"all matching query inverted": {
			queuesToFilter: []*api.Queue{
				{
					Name: "queue-a",
					Labels: map[string]string{
						"armadaproject.io/pool":     "cpu",
						"armadaproject.io/priority": "high",
					},
					Cordoned: false,
				},
				{
					Name: "queue-b",
					Labels: map[string]string{
						"armadaproject.io/pool":     "gpu",
						"armadaproject.io/priority": "medium",
					},
					Cordoned: false,
				},
				{
					Name: "queue-c",
					Labels: map[string]string{
						"armadaproject.io/pool":     "mixed",
						"armadaproject.io/priority": "low",
					},
					Cordoned: true,
				},
			},
			args: &QueueQueryArgs{
				ContainsAllLabels: []string{
					"armadaproject.io/pool",
				},
				InvertResult: true,
			},
			expectedQueueNames: []string{},
			errorExpected:      false,
		},
		"none-matching query inverted": {
			queuesToFilter: []*api.Queue{
				{
					Name: "queue-a",
					Labels: map[string]string{
						"armadaproject.io/pool":     "cpu",
						"armadaproject.io/priority": "high",
					},
					Cordoned: false,
				},
				{
					Name: "queue-b",
					Labels: map[string]string{
						"armadaproject.io/pool":     "gpu",
						"armadaproject.io/priority": "medium",
					},
					Cordoned: false,
				},
				{
					Name: "queue-c",
					Labels: map[string]string{
						"armadaproject.io/pool":     "mixed",
						"armadaproject.io/priority": "low",
					},
					Cordoned: true,
				},
			},
			args: &QueueQueryArgs{
				InQueueNames: []string{"queue-a"},
				OnlyCordoned: true,
				InvertResult: true,
			},
			expectedQueueNames: []string{"queue-a", "queue-b", "queue-c"},
			errorExpected:      false,
		},
		"complex query inverted": {
			queuesToFilter: []*api.Queue{
				{
					Name: "queue-a",
					Labels: map[string]string{
						"armadaproject.io/pool":     "cpu",
						"armadaproject.io/priority": "high",
					},
					Cordoned: false,
				},
				{
					Name: "queue-b",
					Labels: map[string]string{
						"armadaproject.io/pool":     "gpu",
						"armadaproject.io/priority": "medium",
					},
					Cordoned: false,
				},
				{
					Name: "queue-c",
					Labels: map[string]string{
						"armadaproject.io/pool":     "mixed",
						"armadaproject.io/priority": "low",
					},
					Cordoned: true,
				},
			},
			args: &QueueQueryArgs{
				ContainsAllLabels: []string{"armadaproject.io/pool"},
				InQueueNames:      []string{"queue-c"},
				InvertResult:      true,
			},
			expectedQueueNames: []string{"queue-a", "queue-b"},
			errorExpected:      false,
		},
		"complex query inverted 2": {
			queuesToFilter: []*api.Queue{
				{
					Name: "queue-a",
					Labels: map[string]string{
						"armadaproject.io/pool":     "cpu",
						"armadaproject.io/priority": "high",
					},
					Cordoned: false,
				},
				{
					Name: "queue-b",
					Labels: map[string]string{
						"armadaproject.io/pool":     "gpu",
						"armadaproject.io/priority": "medium",
					},
					Cordoned: true,
				},
				{
					Name: "queue-c",
					Labels: map[string]string{
						"armadaproject.io/pool":     "mixed",
						"armadaproject.io/priority": "low",
					},
					Cordoned: false,
				},
			},
			args: &QueueQueryArgs{
				ContainsAllLabels: []string{"armadaproject.io/pool"},
				InQueueNames:      []string{"queue-b", "queue-c"},
				OnlyCordoned:      true,
				InvertResult:      true,
			},
			expectedQueueNames: []string{"queue-a", "queue-c"},
			errorExpected:      false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(tt *testing.T) {
			app := New()

			// No mocking needed for this test thanks to the functional design of QueueAPI
			app.Params.QueueAPI.GetAll = func() ([]*api.Queue, error) {
				return tc.queuesToFilter, nil
			}

			filteredQueues, err := app.getAllQueuesAsAPIQueue(tc.args)
			assert.Equal(tt, tc.errorExpected, err != nil)
			assert.Equal(tt, tc.expectedQueueNames, slices.Map(filteredQueues, func(q *api.Queue) string {
				return q.Name
			}))
		})
	}
}
