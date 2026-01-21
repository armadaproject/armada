package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
)

func TestSchedulingResourceRequirementsFromPodSpec(t *testing.T) {
	tests := map[string]struct {
		input    *v1.PodSpec
		expected *v1.ResourceRequirements
	}{
		"containers and initContainers": {
			input: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":            QuantityWithMilliValue(1),
								"memory":         QuantityWithMilliValue(1),
								"nvidia.com/gpu": QuantityWithMilliValue(1),
							},
							Limits: v1.ResourceList{
								"foo": QuantityWithMilliValue(2),
								"bar": QuantityWithMilliValue(2),
								"baz": QuantityWithMilliValue(2),
							},
						},
					},
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":            QuantityWithMilliValue(2),
								"memory":         QuantityWithMilliValue(2),
								"nvidia.com/gpu": QuantityWithMilliValue(2),
							},
							Limits: v1.ResourceList{
								"foo": QuantityWithMilliValue(3),
								"bar": QuantityWithMilliValue(3),
								"baz": QuantityWithMilliValue(3),
							},
						},
					},
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":            QuantityWithMilliValue(3),
								"memory":         QuantityWithMilliValue(3),
								"nvidia.com/gpu": QuantityWithMilliValue(3),
							},
							Limits: v1.ResourceList{
								"foo": QuantityWithMilliValue(4),
								"bar": QuantityWithMilliValue(4),
								"baz": QuantityWithMilliValue(4),
							},
						},
					},
				},
				InitContainers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":            QuantityWithMilliValue(10),
								"memory":         QuantityWithMilliValue(1),
								"nvidia.com/gpu": QuantityWithMilliValue(1),
							},
							Limits: v1.ResourceList{
								"foo": QuantityWithMilliValue(10),
								"bar": QuantityWithMilliValue(1),
								"baz": QuantityWithMilliValue(1),
							},
						},
					},
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":            QuantityWithMilliValue(1),
								"memory":         QuantityWithMilliValue(10),
								"nvidia.com/gpu": QuantityWithMilliValue(1),
							},
							Limits: v1.ResourceList{
								"foo": QuantityWithMilliValue(1),
								"bar": QuantityWithMilliValue(10),
								"baz": QuantityWithMilliValue(1),
							},
						},
					},
				},
			},
			expected: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":            QuantityWithMilliValue(10),
					"memory":         QuantityWithMilliValue(10),
					"nvidia.com/gpu": QuantityWithMilliValue(6),
				},
				Limits: v1.ResourceList{
					"foo": QuantityWithMilliValue(10),
					"bar": QuantityWithMilliValue(10),
					"baz": QuantityWithMilliValue(9),
				},
			},
		},
		"only containers": {
			input: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":            QuantityWithMilliValue(10),
								"memory":         QuantityWithMilliValue(2),
								"nvidia.com/gpu": QuantityWithMilliValue(1),
							},
							Limits: v1.ResourceList{
								"foo": QuantityWithMilliValue(20),
								"bar": QuantityWithMilliValue(4),
								"baz": QuantityWithMilliValue(2),
							},
						},
					},
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":            QuantityWithMilliValue(1),
								"memory":         QuantityWithMilliValue(5),
								"nvidia.com/gpu": QuantityWithMilliValue(1),
							},
							Limits: v1.ResourceList{
								"foo": QuantityWithMilliValue(2),
								"bar": QuantityWithMilliValue(10),
								"baz": QuantityWithMilliValue(2),
							},
						},
					},
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":            QuantityWithMilliValue(0),
								"memory":         QuantityWithMilliValue(0),
								"nvidia.com/gpu": QuantityWithMilliValue(0),
							},
							Limits: v1.ResourceList{
								"foo": QuantityWithMilliValue(0),
								"bar": QuantityWithMilliValue(0),
								"baz": QuantityWithMilliValue(0),
							},
						},
					},
				},
			},
			expected: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":            QuantityWithMilliValue(11),
					"memory":         QuantityWithMilliValue(7),
					"nvidia.com/gpu": QuantityWithMilliValue(2),
				},
				Limits: v1.ResourceList{
					"foo": QuantityWithMilliValue(22),
					"bar": QuantityWithMilliValue(14),
					"baz": QuantityWithMilliValue(4),
				},
			},
		},
		"only initContainers": {
			input: &v1.PodSpec{
				InitContainers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":            QuantityWithMilliValue(10),
								"memory":         QuantityWithMilliValue(2),
								"nvidia.com/gpu": QuantityWithMilliValue(1),
							},
							Limits: v1.ResourceList{
								"foo": QuantityWithMilliValue(20),
								"bar": QuantityWithMilliValue(4),
								"baz": QuantityWithMilliValue(2),
							},
						},
					},
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":            QuantityWithMilliValue(1),
								"memory":         QuantityWithMilliValue(5),
								"nvidia.com/gpu": QuantityWithMilliValue(1),
							},
							Limits: v1.ResourceList{
								"foo": QuantityWithMilliValue(2),
								"bar": QuantityWithMilliValue(10),
								"baz": QuantityWithMilliValue(2),
							},
						},
					},
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":            QuantityWithMilliValue(0),
								"memory":         QuantityWithMilliValue(0),
								"nvidia.com/gpu": QuantityWithMilliValue(0),
							},
							Limits: v1.ResourceList{
								"foo": QuantityWithMilliValue(0),
								"bar": QuantityWithMilliValue(0),
								"baz": QuantityWithMilliValue(0),
							},
						},
					},
				},
			},
			expected: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":            QuantityWithMilliValue(10),
					"memory":         QuantityWithMilliValue(5),
					"nvidia.com/gpu": QuantityWithMilliValue(1),
				},
				Limits: v1.ResourceList{
					"foo": QuantityWithMilliValue(20),
					"bar": QuantityWithMilliValue(10),
					"baz": QuantityWithMilliValue(2),
				},
			},
		},
		"native sidecar init containers are summed with main containers": {
			input: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    QuantityWithMilliValue(2000),
								"memory": QuantityWithMilliValue(1000),
							},
							Limits: v1.ResourceList{
								"cpu":    QuantityWithMilliValue(2000),
								"memory": QuantityWithMilliValue(1000),
							},
						},
					},
				},
				InitContainers: []v1.Container{
					{
						RestartPolicy: restartPolicyAlways(),
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    QuantityWithMilliValue(1000),
								"memory": QuantityWithMilliValue(500),
							},
							Limits: v1.ResourceList{
								"cpu":    QuantityWithMilliValue(1000),
								"memory": QuantityWithMilliValue(500),
							},
						},
					},
				},
			},
			// main (2000, 1000) + sidecar (1000, 500) = (3000, 1500)
			expected: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    QuantityWithMilliValue(3000),
					"memory": QuantityWithMilliValue(1500),
				},
				Limits: v1.ResourceList{
					"cpu":    QuantityWithMilliValue(3000),
					"memory": QuantityWithMilliValue(1500),
				},
			},
		},
		"mixed native sidecar and classic init containers": {
			input: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    QuantityWithMilliValue(2000),
								"memory": QuantityWithMilliValue(1000),
							},
							Limits: v1.ResourceList{
								"cpu":    QuantityWithMilliValue(2000),
								"memory": QuantityWithMilliValue(1000),
							},
						},
					},
				},
				InitContainers: []v1.Container{
					{
						RestartPolicy: restartPolicyAlways(),
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    QuantityWithMilliValue(1000),
								"memory": QuantityWithMilliValue(500),
							},
							Limits: v1.ResourceList{
								"cpu":    QuantityWithMilliValue(1000),
								"memory": QuantityWithMilliValue(500),
							},
						},
					},
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    QuantityWithMilliValue(5000),
								"memory": QuantityWithMilliValue(100),
							},
							Limits: v1.ResourceList{
								"cpu":    QuantityWithMilliValue(5000),
								"memory": QuantityWithMilliValue(100),
							},
						},
					},
				},
			},
			// Running total: main (2000, 1000) + sidecar (1000, 500) = (3000, 1500)
			// Classic init: (5000, 100) -> max with running: (5000, 1500)
			expected: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    QuantityWithMilliValue(5000),
					"memory": QuantityWithMilliValue(1500),
				},
				Limits: v1.ResourceList{
					"cpu":    QuantityWithMilliValue(5000),
					"memory": QuantityWithMilliValue(1500),
				},
			},
		},
		"multiple native sidecars are all summed": {
			input: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu": QuantityWithMilliValue(1000),
							},
							Limits: v1.ResourceList{
								"cpu": QuantityWithMilliValue(1000),
							},
						},
					},
				},
				InitContainers: []v1.Container{
					{
						RestartPolicy: restartPolicyAlways(),
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu": QuantityWithMilliValue(500),
							},
							Limits: v1.ResourceList{
								"cpu": QuantityWithMilliValue(500),
							},
						},
					},
					{
						RestartPolicy: restartPolicyAlways(),
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu": QuantityWithMilliValue(500),
							},
							Limits: v1.ResourceList{
								"cpu": QuantityWithMilliValue(500),
							},
						},
					},
				},
			},
			// main (1000) + sidecar1 (500) + sidecar2 (500) = 2000
			expected: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu": QuantityWithMilliValue(2000),
				},
				Limits: v1.ResourceList{
					"cpu": QuantityWithMilliValue(2000),
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, SchedulingResourceRequirementsFromPodSpec(tc.input))
		})
	}
}

// QuantityWithMilliValue returns a new quantity with the provided milli value assigned to it.
// Using this instead of resource.MustParse avoids populating the cached string field,
// which may cause assert.Equal to return false for quantities with equal value but where
// either quantity doesn't have a cached string representation.
func QuantityWithMilliValue(v int64) resource.Quantity {
	q := resource.Quantity{}
	q.SetMilli(v)
	return q
}

// restartPolicyAlways returns a pointer to ContainerRestartPolicyAlways for use in tests.
func restartPolicyAlways() *v1.ContainerRestartPolicy {
	policy := v1.ContainerRestartPolicyAlways
	return &policy
}
