package queue

import (
	"encoding/json"
	"github.com/armadaproject/armada/pkg/api"
	"reflect"
	"testing"
	"testing/quick"
)

func TestQueue(t *testing.T) {
	testCase := func(queue1 Queue) bool {
		queue2, err := NewQueue(queue1.ToAPI())
		if err != nil {
			t.Error(err)
			return false
		}

		return reflect.DeepEqual(queue1, queue2)
	}

	if err := quick.Check(testCase, nil); err != nil {
		t.Fatal(err)
	}
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

		// json.Unmarshal will unmarshall empty maps to nil
		// TODO: refactor these tests/code so this nonsense is not necessary
		newResourceLimits := make(map[string]api.PriorityClassResourceLimits, len(queue2.ResourceLimitsByPriorityClassName))
		for k1, v := range queue2.ResourceLimitsByPriorityClassName {
			if v.MaximumResourceFractionByPool == nil {
				v.MaximumResourceFractionByPool = map[string]*api.PriorityClassPoolResourceLimits{}
			}
			for k, v2 := range v.MaximumResourceFractionByPool {
				if v2 != nil && v2.MaximumResourceFraction == nil {
					v.MaximumResourceFractionByPool[k] = &api.PriorityClassPoolResourceLimits{
						MaximumResourceFraction: map[string]float64{},
					}
				}
				if v2 != nil && v2.MaximumResourceFraction == nil {
					v2.MaximumResourceFraction = map[string]float64{}
				}
			}
			if v.MaximumResourceFraction == nil {
				v.MaximumResourceFraction = map[string]float64{}
			}
			newResourceLimits[k1] = v
		}
		queue2.ResourceLimitsByPriorityClassName = newResourceLimits

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
