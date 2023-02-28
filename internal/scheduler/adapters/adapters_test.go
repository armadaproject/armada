package adapters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestPriorityFromPodSpec(t *testing.T) {
	// test to check if PriorityFromPodSpec function returns a priority number of 0 and boolean value
	// of false when podSpec is nil
	var podSpec *v1.PodSpec = nil
	// priority number is of type int32 to conform with the priority number type in the k8s API
	var priority int32 = 1
	priorityByPriorityClassName := map[string]configuration.PriorityClass{
		"priority-0": {int32(0), true, nil},
		"priority-1": {int32(1), true, nil},
		"priority-2": {int32(2), true, nil},
		"priority-3": {int32(3), false, nil},
	}

	priorityNumber, isPrioritySet := PriorityFromPodSpec(podSpec, priorityByPriorityClassName)
	assert.Equal(t, int32(0), priorityNumber)
	assert.False(t, isPrioritySet)

	// test to ensure that the function PriorityFromPodSpec returns the podSpec's Priority and  a boolean value of true
	// when podSpec has a value for the Priority field.
	podSpec = &v1.PodSpec{
		Priority: &priority,
	}
	priorityNumber, isPrioritySet = PriorityFromPodSpec(podSpec, priorityByPriorityClassName)
	assert.Equal(t, priorityNumber, priority)
	assert.True(t, isPrioritySet)

	podSpec = &v1.PodSpec{
		PriorityClassName: "priority-3",
	}
	priorityNumber, isPrioritySet = PriorityFromPodSpec(podSpec, priorityByPriorityClassName)
	priorityObj, ok := priorityByPriorityClassName[podSpec.PriorityClassName]
	require.True(t, ok)

	assert.Equal(t, priorityNumber, priorityObj.Priority)
	assert.True(t, isPrioritySet)

	// test to ensure that the PriorityFromPodSpec function returns 0 and a boolean value of false when podSpec's Priority is nil
	podSpec = &v1.PodSpec{
		Priority: nil,
	}
	priorityNumber, isPrioritySet = PriorityFromPodSpec(podSpec, priorityByPriorityClassName)
	assert.Equal(t, int32(0), priorityNumber)
	assert.False(t, isPrioritySet)
}

func TestV1ResourceListFromComputeResources(t *testing.T) {
	var armandaResources armadaresource.ComputeResources = map[string]resource.Quantity{
		"cpu":    *resource.NewMilliQuantity(5300, resource.DecimalSI),
		"memory": *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI),
	}
	rv := v1ResourceListFromComputeResources(armandaResources)

	expectedRv := v1.ResourceList{
		v1.ResourceName("cpu"):    *resource.NewMilliQuantity(5300, resource.DecimalSI),
		v1.ResourceName("memory"): *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI),
	}
	assert.Equal(t, expectedRv, rv)
}

func TestPodRequirementsFromPodSpec(t *testing.T) {
	var priority int32 = 1
	priorityByPriorityClassName := map[string]configuration.PriorityClass{
		"priority-0": {0, true, nil},
		"priority-1": {1, true, nil},
		"priority-2": {2, true, nil},
		"priority-3": {3, false, nil},
	}
	podSpec := &v1.PodSpec{
		Priority: &priority,
		Containers: []v1.Container{
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
		},
	}

	expectedResourceRequirement := v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceName("cpu"):    *resource.NewMilliQuantity(5300, resource.DecimalSI),
			v1.ResourceName("memory"): *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI),
		},
		Requests: v1.ResourceList{
			v1.ResourceName("cpu"):    *resource.NewMilliQuantity(300, resource.DecimalSI),
			v1.ResourceName("memory"): *resource.NewQuantity(2*1024*1024*1024, resource.BinarySI),
		},
	}
	expectedScheduler := &schedulerobjects.PodRequirements{
		Priority:             priority,
		ResourceRequirements: expectedResourceRequirement,
		PreemptionPolicy:     string(v1.PreemptLowerPriority),
	}

	scheduler := PodRequirementsFromPodSpec(podSpec, priorityByPriorityClassName)

	assert.Equal(t, scheduler, expectedScheduler)
}

func TestPodRequirementsFromPod(t *testing.T) {
	var priority int32 = 1
	priorityByPriorityClassName := map[string]configuration.PriorityClass{
		"priority-0": {0, true, nil},
		"priority-1": {1, true, nil},
		"priority-2": {2, true, nil},
		"priority-3": {3, false, nil},
	}
	podSpec := &v1.PodSpec{
		Priority: &priority,
		Containers: []v1.Container{
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
		},
	}
	pod := v1.Pod{
		Spec: *podSpec,
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				configuration.GangIdAnnotation:          "gang-id",
				configuration.GangCardinalityAnnotation: "1",
			},
		},
	}
	rv := PodRequirementsFromPod(&pod, priorityByPriorityClassName)
	rv.Annotations["something"] = "test"
	assert.Len(t, pod.Annotations, 2)
}
