package adapters

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

// PodRequirementsFromPod function creates the schedulerobjects and creates a value for the
// annotation field by supplying it with a cloned value of pod.Annotations
func PodRequirementsFromPod(pod *v1.Pod, priorityByPriorityClassName map[string]configuration.PriorityClass) *schedulerobjects.PodRequirements {
	rv := PodRequirementsFromPodSpec(&pod.Spec, priorityByPriorityClassName)
	rv.Annotations = maps.Clone(pod.Annotations)
	return rv
}

// PodRequirementsFromPodSpec function returns *schedulerobjects.PodRequirements for podSpec.
// An error is logged if the podSpec uses an unknown priority class.
// This function may mutate podSpec.
func PodRequirementsFromPodSpec(podSpec *v1.PodSpec, priorityByPriorityClassName map[string]configuration.PriorityClass) *schedulerobjects.PodRequirements {
	priority, ok := api.PriorityFromPodSpec(podSpec, priorityByPriorityClassName)
	if priorityByPriorityClassName != nil && !ok {
		// Ignore this error if priorityByPriorityClassName is explicitly set to nil.
		// We assume that in this case the caller is sure the priority does not need to be set.
		err := errors.Errorf("unknown priorityClassName %s", podSpec.PriorityClassName)
		logging.WithStacktrace(logrus.NewEntry(logrus.New()), err).Error("failed to get priority from priorityClassName")
	}
	preemptionPolicy := string(v1.PreemptLowerPriority)
	if podSpec.PreemptionPolicy != nil {
		preemptionPolicy = string(*podSpec.PreemptionPolicy)
	}
	return &schedulerobjects.PodRequirements{
		NodeSelector:         podSpec.NodeSelector,
		Affinity:             podSpec.Affinity,
		Tolerations:          podSpec.Tolerations,
		Priority:             priority,
		PreemptionPolicy:     preemptionPolicy,
		ResourceRequirements: api.SchedulingResourceRequirementsFromPodSpec(podSpec),
	}
}
