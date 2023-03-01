package adapters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

var (
	priorityByPriorityClassName = map[string]configuration.PriorityClass{
		"priority-0": {0, true, nil},
		"priority-1": {1, true, nil},
		"priority-2": {2, true, nil},
		"priority-3": {3, false, nil},
	}

	priority int32 = 1
)

func TestPriorityFromPodSpec(t *testing.T) {
	tests := []struct {
		name                     string
		podSpec                  *v1.PodSpec
		expectedPriorityNumber   int32
		expectedCheckForPriority bool
	}{
		{
			name:                     "Podspec is nil",
			podSpec:                  nil,
			expectedPriorityNumber:   0,
			expectedCheckForPriority: false,
		},
		{
			name: "Podspec has priority field",
			podSpec: &v1.PodSpec{
				Priority: &priority,
			},
			expectedPriorityNumber:   priority,
			expectedCheckForPriority: true,
		},
		{
			name: "Podspec has priorityClassName field",
			podSpec: &v1.PodSpec{
				PriorityClassName: "priority-3",
			},
			expectedPriorityNumber:   3,
			expectedCheckForPriority: true,
		},
		{
			name: "Podspec has a nil value for the priority field",
			podSpec: &v1.PodSpec{
				Priority: nil,
			},
			expectedPriorityNumber:   0,
			expectedCheckForPriority: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wantedPriorityNumber, wantedCheckForPriority := PriorityFromPodSpec(test.podSpec, priorityByPriorityClassName)
			assert.Equal(t, test.expectedCheckForPriority, wantedCheckForPriority)
			assert.Equal(t, test.expectedPriorityNumber, wantedPriorityNumber)
		})
	}
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
	// Ensures that any modification made to the returned value of PodRequirementsFromPod function, "rv", does not
	// affect the original pod definition. This assertion checks if the length of "pod.Annotation" is altered
	// in view of the modification made to "rv" above.
	assert.Len(t, pod.Annotations, 2)
}
