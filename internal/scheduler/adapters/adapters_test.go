package adapters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

const (
	PriorityClass0 = "priority-0"
	PriorityClass1 = "priority-1"
	PriorityClass2 = "priority-2"
	PriorityClass3 = "priority-3"
)

var (
	priorityByPriorityClassName = map[string]types.PriorityClass{
		PriorityClass0: {Priority: 0, Preemptible: true},
		PriorityClass1: {Priority: 1, Preemptible: true},
		PriorityClass2: {Priority: 2, Preemptible: true},
		PriorityClass3: {Priority: 3, Preemptible: false},
	}

	priority int32 = 1

	containerObj = []v1.Container{
		{
			Resources: v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceName("cpu"):    *resource.NewMilliQuantity(5300, resource.DecimalSI),
					v1.ResourceName("memory"): *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI),
				},
				Requests: v1.ResourceList{
					v1.ResourceName("cpu"):    *resource.NewMilliQuantity(300, resource.DecimalSI),
					v1.ResourceName("memory"): *resource.NewQuantity(2*1024*1024*1024, resource.BinarySI),
				},
			},
		},
	}

	expectedResourceRequirement = v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceName("cpu"):    *resource.NewMilliQuantity(5300, resource.DecimalSI),
			v1.ResourceName("memory"): *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI),
		},
		Requests: v1.ResourceList{
			v1.ResourceName("cpu"):    *resource.NewMilliQuantity(300, resource.DecimalSI),
			v1.ResourceName("memory"): *resource.NewQuantity(2*1024*1024*1024, resource.BinarySI),
		},
	}
	expectedScheduler = &schedulerobjects.PodRequirements{
		ResourceRequirements: expectedResourceRequirement,
		PreemptionPolicy:     string(v1.PreemptLowerPriority),
	}
)

func TestPodRequirementsFromPodSpecPreemptionPolicy(t *testing.T) {
	preemptNever := v1.PreemptNever
	tests := []struct {
		name             string
		podspec          v1.PodSpec
		preemptionpolicy v1.PreemptionPolicy
	}{
		{
			name: "Preemption policy is not nil",
			podspec: v1.PodSpec{
				Priority:         &priority,
				Containers:       containerObj,
				PreemptionPolicy: &preemptNever,
			},
			preemptionpolicy: preemptNever,
		},

		{
			name: "Preemption policy is nil",
			podspec: v1.PodSpec{
				Priority:   &priority,
				Containers: containerObj,
			},
			preemptionpolicy: v1.PreemptLowerPriority,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheduler := PodRequirementsFromPodSpec(&test.podspec)
			assert.Equal(t, scheduler.PreemptionPolicy, string(test.preemptionpolicy))
		})
	}
}

func TestK8sResourceListToMap(t *testing.T) {
	result := K8sResourceListToMap(v1.ResourceList{
		"one": resource.MustParse("1"),
		"two": resource.MustParse("2"),
	})
	expected := map[string]resource.Quantity{
		"one": resource.MustParse("1"),
		"two": resource.MustParse("2"),
	}

	assert.Equal(t, expected, result)
}

func TestK8sResourceListToMap_PreservesNil(t *testing.T) {
	assert.Nil(t, K8sResourceListToMap(nil))
}
