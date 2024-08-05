package adapters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var (
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
