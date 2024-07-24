package queue

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
	"testing/quick"
)

func TestQueue(t *testing.T) {
	testCaseIgnoreLabels := func(queue1 Queue) bool {
		queue1.Labels = nil
		queue2, err := NewQueue(queue1.ToAPI())
		if err != nil {
			t.Error(err)
			return false
		}

		return reflect.DeepEqual(queue1, queue2)
	}

	if err := quick.Check(testCaseIgnoreLabels, nil); err != nil {
		t.Fatal(err)
	}
}

func TestQueueWithLabels(t *testing.T) {
	queue1 := Queue{
		Name:           "queue-a",
		PriorityFactor: 100,
		Permissions:    []Permissions{},
		Labels:         []string{"armadaproject.io/gpu-category=gang-user", "armadaproject.io/priority=critical"},
	}
	queue2, err := NewQueue(queue1.ToAPI())
	if err != nil {
		t.Error(err)
	}

	require.True(t, reflect.DeepEqual(queue1, queue2))
}

func TestQueueWithIncorrectLabels(t *testing.T) {
	queue1 := Queue{
		Name:           "queue-a",
		PriorityFactor: 100,
		Labels:         []string{"armadaproject.io/not-key-value"},
	}
	_, err := NewQueue(queue1.ToAPI())
	require.Error(t, err)
}

func TestQueueMarshalUnmarshal(t *testing.T) {
	testCase := func(queue1 Queue) bool {
		var queue2 Queue
		data, err := json.Marshal(queue1)
		if err != nil {
			t.Error(err)
			return false
		}

		if err := json.Unmarshal(data, &queue2); err != nil {
			t.Error(err)
			return false
		}

		return reflect.DeepEqual(queue1, queue2)
	}

	if err := quick.Check(testCase, nil); err != nil {
		t.Fatal(err)
	}
}

func TestQueueHasPermissionTrue(t *testing.T) {
	testCase := func(queue Queue) bool {
		for _, permissions := range queue.Permissions {
			for _, subject := range permissions.Subjects {
				for _, verb := range permissions.Verbs {
					if !queue.HasPermission(subject, verb) {
						return false
					}
				}
			}
		}
		return true
	}

	if err := quick.Check(testCase, nil); err != nil {
		t.Fatal(err)
	}
}

func TestQueueHasPermissionFalse(t *testing.T) {
	testCases := map[string]interface{}{
		"UserDoesntHavePermissions": func(queue Queue) bool {
			return !queue.HasPermission(
				PermissionSubject{Kind: PermissionSubjectKindUser, Name: "quant"},
				PermissionVerbCancel,
			)
		},
		"GroupDoesntHavePermissions": func(queue Queue) bool {
			return !queue.HasPermission(
				PermissionSubject{Kind: PermissionSubjectKindGroup, Name: "quants"},
				PermissionVerbCancel,
			)
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(sub *testing.T) {
			if err := quick.Check(testCase, nil); err != nil {
				t.Fatal(err)
			}
		})
	}
}
