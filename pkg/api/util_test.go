package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

const (
	PriorityClassEmpty = ""
	PriorityClass0     = "priority-0"
	PriorityClass1     = "priority-1"
	PriorityClass2     = "priority-2"
	PriorityClass3     = "priority-3"
)

var (
	TestPriorityClasses = map[string]configuration.PriorityClass{
		PriorityClassEmpty: {Priority: 0, Preemptible: true},
		PriorityClass0:     {Priority: 0, Preemptible: true},
		PriorityClass1:     {Priority: 1, Preemptible: true},
		PriorityClass2:     {Priority: 2, Preemptible: true},
		PriorityClass3:     {Priority: 3, Preemptible: false},
	}
	TestDefaultPriorityClass = PriorityClass3
	TestPriorities           = []int32{0, 1, 2, 3}
)

func TestSchedulingResourceRequirementsFromPodSpec(t *testing.T) {
	tests := map[string]struct {
		input    *v1.PodSpec
		expected v1.ResourceRequirements
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
			expected: v1.ResourceRequirements{
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
			expected: v1.ResourceRequirements{
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
			expected: v1.ResourceRequirements{
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
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, SchedulingResourceRequirementsFromPodSpec(tc.input))
		})
	}
}

// quantityWithMilliValue returns a new quantity with the provided milli value assigned to it.
// Using this instead of resource.MustParse avoids populating the cached string field,
// which may cause assert.Equal to return false for quantities with equal value but where
// either quantity doesn't have a cached string representation.
func QuantityWithMilliValue(v int64) resource.Quantity {
	q := resource.Quantity{}
	q.SetMilli(v)
	return q
}

func TestJobGetRequirements(t *testing.T) {
	tests := map[string]struct {
		job      *Job
		expected *schedulerobjects.JobSchedulingInfo
	}{
		"queue priority": {
			job: &Job{
				Priority: 10,
				PodSpec:  &v1.PodSpec{
					// Priority: pointerFromValue(int32(10)),
					// PriorityClassName: ,
				},
			},
			expected: &schedulerobjects.JobSchedulingInfo{
				Priority: 10,
				ObjectRequirements: []*schedulerobjects.ObjectRequirements{
					{
						&schedulerobjects.ObjectRequirements_PodRequirements{
							PodRequirements: &schedulerobjects.PodRequirements{
								PreemptionPolicy: string(v1.PreemptLowerPriority),
								ResourceRequirements: v1.ResourceRequirements{
									Requests: make(v1.ResourceList),
									Limits:   make(v1.ResourceList),
								},
							},
						},
					},
				},
			},
		},
		"priorityClass priority": {
			job: &Job{
				PodSpec: &v1.PodSpec{
					PriorityClassName: PriorityClass1,
				},
			},
			expected: &schedulerobjects.JobSchedulingInfo{
				PriorityClassName: PriorityClass1,
				ObjectRequirements: []*schedulerobjects.ObjectRequirements{
					{
						&schedulerobjects.ObjectRequirements_PodRequirements{
							PodRequirements: &schedulerobjects.PodRequirements{
								Priority:         1,
								PreemptionPolicy: string(v1.PreemptLowerPriority),
								ResourceRequirements: v1.ResourceRequirements{
									Requests: make(v1.ResourceList),
									Limits:   make(v1.ResourceList),
								},
							},
						},
					},
				},
			},
		},
		"preemptionPolicy": {
			job: &Job{
				PodSpec: &v1.PodSpec{
					PreemptionPolicy: pointerFromValue(v1.PreemptNever),
				},
			},
			expected: &schedulerobjects.JobSchedulingInfo{
				ObjectRequirements: []*schedulerobjects.ObjectRequirements{
					{
						&schedulerobjects.ObjectRequirements_PodRequirements{
							PodRequirements: &schedulerobjects.PodRequirements{
								PreemptionPolicy: string(v1.PreemptNever),
								ResourceRequirements: v1.ResourceRequirements{
									Requests: make(v1.ResourceList),
									Limits:   make(v1.ResourceList),
								},
							},
						},
					},
				},
			},
		},
		"targeting": {
			job: &Job{
				PodSpec: &v1.PodSpec{
					NodeSelector: map[string]string{"label": "value"},
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key: "affinityKey",
											},
										},
									},
								},
							},
						},
					},
					Tolerations: []v1.Toleration{
						{
							Key: "tolerationKey",
						},
					},
				},
			},
			expected: &schedulerobjects.JobSchedulingInfo{
				ObjectRequirements: []*schedulerobjects.ObjectRequirements{
					{
						&schedulerobjects.ObjectRequirements_PodRequirements{
							PodRequirements: &schedulerobjects.PodRequirements{
								NodeSelector: map[string]string{"label": "value"},
								Affinity: &v1.Affinity{
									NodeAffinity: &v1.NodeAffinity{
										RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
											NodeSelectorTerms: []v1.NodeSelectorTerm{
												{
													MatchExpressions: []v1.NodeSelectorRequirement{
														{
															Key: "affinityKey",
														},
													},
												},
											},
										},
									},
								},
								Tolerations: []v1.Toleration{
									{
										Key: "tolerationKey",
									},
								},
								PreemptionPolicy: string(v1.PreemptLowerPriority),
								ResourceRequirements: v1.ResourceRequirements{
									Requests: make(v1.ResourceList),
									Limits:   make(v1.ResourceList),
								},
							},
						},
					},
				},
			},
		},
		"annotations": {
			job: &Job{
				Annotations: map[string]string{"key": "value"},
				PodSpec:     &v1.PodSpec{},
			},
			expected: &schedulerobjects.JobSchedulingInfo{
				ObjectRequirements: []*schedulerobjects.ObjectRequirements{
					{
						&schedulerobjects.ObjectRequirements_PodRequirements{
							PodRequirements: &schedulerobjects.PodRequirements{
								Annotations:      map[string]string{"key": "value"},
								PreemptionPolicy: string(v1.PreemptLowerPriority),
								ResourceRequirements: v1.ResourceRequirements{
									Requests: make(v1.ResourceList),
									Limits:   make(v1.ResourceList),
								},
							},
						},
					},
				},
			},
		},
		"schedulingResourceRequirements": {
			job: &Job{
				PodSpec: &v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{"foo": QuantityWithMilliValue(1000)},
								Limits:   v1.ResourceList{"bar": QuantityWithMilliValue(2000)},
							},
						},
					},
				},
			},
			expected: &schedulerobjects.JobSchedulingInfo{
				ObjectRequirements: []*schedulerobjects.ObjectRequirements{
					{
						&schedulerobjects.ObjectRequirements_PodRequirements{
							PodRequirements: &schedulerobjects.PodRequirements{
								PreemptionPolicy: string(v1.PreemptLowerPriority),
								ResourceRequirements: v1.ResourceRequirements{
									Requests: v1.ResourceList{"foo": QuantityWithMilliValue(1000)},
									Limits:   v1.ResourceList{"bar": QuantityWithMilliValue(2000)},
								},
							},
						},
					},
				},
			},
		},
		"schedulingResourceRequirements pre-computed requests": {
			job: &Job{
				PodSpec: &v1.PodSpec{},
				SchedulingResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{"foo": resource.MustParse("1")},
				},
			},
			expected: &schedulerobjects.JobSchedulingInfo{
				ObjectRequirements: []*schedulerobjects.ObjectRequirements{
					{
						&schedulerobjects.ObjectRequirements_PodRequirements{
							PodRequirements: &schedulerobjects.PodRequirements{
								PreemptionPolicy: string(v1.PreemptLowerPriority),
								ResourceRequirements: v1.ResourceRequirements{
									Requests: v1.ResourceList{"foo": resource.MustParse("1")},
									Limits:   nil,
								},
							},
						},
					},
				},
			},
		},
		"schedulingResourceRequirements pre-computed limits": {
			job: &Job{
				PodSpec: &v1.PodSpec{},
				SchedulingResourceRequirements: v1.ResourceRequirements{
					Limits: v1.ResourceList{"foo": resource.MustParse("1")},
				},
			},
			expected: &schedulerobjects.JobSchedulingInfo{
				ObjectRequirements: []*schedulerobjects.ObjectRequirements{
					{
						&schedulerobjects.ObjectRequirements_PodRequirements{
							PodRequirements: &schedulerobjects.PodRequirements{
								PreemptionPolicy: string(v1.PreemptLowerPriority),
								ResourceRequirements: v1.ResourceRequirements{
									Requests: nil,
									Limits:   v1.ResourceList{"foo": resource.MustParse("1")},
								},
							},
						},
					},
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.job.GetRequirements(TestPriorityClasses))
		})
	}
}

func TestPriorityFromPodSpec(t *testing.T) {
	tests := map[string]struct {
		podSpec          *v1.PodSpec
		expectedPriority int32
		expectedOk       bool
	}{
		"nil podSpec": {
			podSpec:          nil,
			expectedPriority: 0,
			expectedOk:       false,
		},
		"priority already set": {
			podSpec: &v1.PodSpec{
				Priority:          pointerFromValue(int32(1)),
				PriorityClassName: PriorityClass2,
			},
			expectedPriority: 1,
			expectedOk:       true,
		},
		"existing priorityClass": {
			podSpec: &v1.PodSpec{
				PriorityClassName: PriorityClass2,
			},
			expectedPriority: 2,
			expectedOk:       true,
		},
		"non-existing priorityClass": {
			podSpec: &v1.PodSpec{
				PriorityClassName: "does not exist",
			},
			expectedPriority: 0,
			expectedOk:       false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			p, ok := PriorityFromPodSpec(tc.podSpec, TestPriorityClasses)
			assert.Equal(t, tc.expectedPriority, p)
			assert.Equal(t, tc.expectedOk, ok)
		})
	}
}

func pointerFromValue[T any](v T) *T {
	return &v
}
