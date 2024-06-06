package adapters

import (
	"io"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"

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

func TestPodRequirementsFromPodSpecPriorityByPriorityClassName(t *testing.T) {
	tests := []struct {
		name                        string
		podspec                     v1.PodSpec
		priorityByPriorityClassName map[string]types.PriorityClass
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
			r, w, _ := os.Pipe()
			// Stderr from this function would be written to file w
			logrus.SetOutput(w)
			scheduler := PodRequirementsFromPodSpec(&test.podspec, test.priorityByPriorityClassName)
			// Closing file, w
			err := w.Close()
			require.NoError(t, err)
			// Reading from file
			out, _ := io.ReadAll(r)
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
				Priority:          pointer.Int32(1),
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
			p, ok := PriorityFromPodSpec(tc.podSpec, priorityByPriorityClassName)
			assert.Equal(t, tc.expectedPriority, p)
			assert.Equal(t, tc.expectedOk, ok)
		})
	}
}
