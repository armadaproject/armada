package adapters

import (
	"github.com/stretchr/testify/require"
	"io"
	"os"
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

func TestPodRequirementsFromPodSpecPriorityByPriorityClassName(t *testing.T) {

	tests := []struct {
		name                        string
		podspec                     v1.PodSpec
		priorityByPriorityClassName map[string]configuration.PriorityClass
		loggedError                 bool
		priority                    int32
	}{
		{
			name: "PriorityClassName not present in priorityByPriorityClassName map",
			podspec: v1.PodSpec{
				PriorityClassName: "priority-8",
				Containers:        containerObj,
			},
			priorityByPriorityClassName: priorityByPriorityClassName,
			loggedError:                 true,
			priority:                    0,
		},
		{
			name: "PriorityByPriorityClassName map is nil",
			podspec: v1.PodSpec{
				PriorityClassName: "priority-3",
				Containers:        containerObj,
			},
			priorityByPriorityClassName: nil,
			loggedError:                 false,
			priority:                    0,
		},
		{
			name: "Priority is set directly on podspec",
			podspec: v1.PodSpec{
				Priority:   &priority,
				Containers: containerObj,
			},
			priorityByPriorityClassName: priorityByPriorityClassName,
			loggedError:                 false,
			priority:                    priority,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Creating backup for stderr
			old := os.Stderr
			r, w, _ := os.Pipe()
			// Assigning stderr to file, w
			os.Stderr = w
			// Stderr from this function would be written to file w
			scheduler := PodRequirementsFromPodSpec(&test.podspec, test.priorityByPriorityClassName)
			// Closing file, w
			err := w.Close()
			require.NoError(t, err)
			// Reading from file
			out, _ := io.ReadAll(r)
			// Restoring stderr
			os.Stderr = old
			expectedScheduler.Priority = test.priority
			assert.Equal(t, scheduler, expectedScheduler)
			// if loggedError is true, bytes should be written to stderr,
			// Otherwise, no byte is expected to be written to stderr
			if test.loggedError {
				assert.NotEqual(t, len(out), 0)
			} else {
				assert.Equal(t, len(out), 0)
			}
		})
	}
}

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
			scheduler := PodRequirementsFromPodSpec(&test.podspec, priorityByPriorityClassName)
			assert.Equal(t, scheduler.PreemptionPolicy, string(test.preemptionpolicy))
		})
	}

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
