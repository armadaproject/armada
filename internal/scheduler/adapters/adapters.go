package adapters

import (
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// PodRequirementsFromPod function creates the schedulerobjects and creates a value for the
// annotation field by supplying it with a cloned value of pod.Annotations
func PodRequirementsFromPod(pod *v1.Pod, priorityByPriorityClassName map[string]types.PriorityClass) *schedulerobjects.PodRequirements {
	rv := PodRequirementsFromPodSpec(&pod.Spec, priorityByPriorityClassName)
	rv.Annotations = maps.Clone(pod.Annotations)
	return rv
}

// PodRequirementsFromPodSpec function returns *schedulerobjects.PodRequirements for podSpec.
// An error is logged if the podSpec uses an unknown priority class.
// This function may mutate podSpec.
func PodRequirementsFromPodSpec(podSpec *v1.PodSpec, priorityByPriorityClassName map[string]types.PriorityClass) *schedulerobjects.PodRequirements {
	priority, ok := PriorityFromPodSpec(podSpec, priorityByPriorityClassName)
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

// SchedulingInfoFromSubmitJob returns a minimal representation of a job containing only the info needed by the scheduler.
func SchedulingInfoFromSubmitJob(submitJob *armadaevents.SubmitJob, submitTime time.Time, priorityClasses map[string]types.PriorityClass) (*schedulerobjects.JobSchedulingInfo, error) {
	// Component common to all jobs.
	schedulingInfo := &schedulerobjects.JobSchedulingInfo{
		Lifetime:        submitJob.Lifetime,
		AtMostOnce:      submitJob.AtMostOnce,
		Preemptible:     submitJob.Preemptible,
		ConcurrencySafe: submitJob.ConcurrencySafe,
		SubmitTime:      submitTime,
		Priority:        submitJob.Priority,
		Version:         0,
		QueueTtlSeconds: submitJob.QueueTtlSeconds,
	}

	// Scheduling requirements specific to the objects that make up this job.
	switch object := submitJob.MainObject.Object.(type) {
	case *armadaevents.KubernetesMainObject_PodSpec:
		podSpec := object.PodSpec.PodSpec
		schedulingInfo.PriorityClassName = podSpec.PriorityClassName
		podRequirements := PodRequirementsFromPodSpec(podSpec, priorityClasses)
		if submitJob.ObjectMeta != nil {
			podRequirements.Annotations = maps.Clone(submitJob.ObjectMeta.Annotations)
		}
		if submitJob.MainObject.ObjectMeta != nil {
			if podRequirements.Annotations == nil {
				podRequirements.Annotations = make(map[string]string, len(submitJob.MainObject.ObjectMeta.Annotations))
			}
			maps.Copy(podRequirements.Annotations, submitJob.MainObject.ObjectMeta.Annotations)
		}
		schedulingInfo.ObjectRequirements = append(
			schedulingInfo.ObjectRequirements,
			&schedulerobjects.ObjectRequirements{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: podRequirements,
				},
			},
		)
	default:
		return nil, errors.Errorf("unsupported object type %T", object)
	}
	return schedulingInfo, nil
}

// PriorityFromPodSpec returns the priority in a pod spec.
// If priority is set directly, that value is returned.
// Otherwise, it returns the value of the key podSpec.
// In both cases the value along with true boolean is returned.
// PriorityClassName in priorityByPriorityClassName map.
// If no priority is set for the pod spec, 0 along with a false boolean would be returned
func PriorityFromPodSpec(podSpec *v1.PodSpec, priorityClasses map[string]types.PriorityClass) (int32, bool) {
	// If there's no podspec there's nothing we can do
	if podSpec == nil {
		return 0, false
	}

	// If a priority is directly specified, use that
	if podSpec.Priority != nil {
		return *podSpec.Priority, true
	}

	// If we find a priority class use that
	priorityClass, ok := priorityClasses[podSpec.PriorityClassName]
	if ok {
		return priorityClass.Priority, true
	}

	// Couldn't find anything
	return 0, false
}
