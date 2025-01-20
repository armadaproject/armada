package internaltypes

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	corev1 "k8s.io/api/core/v1"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// JobSchedulingInfo is a minimal representation of job requirements that the scheduler uses for scheduling
type JobSchedulingInfo struct {
	Lifetime          uint32
	PriorityClassName string
	SubmitTime        time.Time
	Priority          uint32
	PodRequirements   *PodRequirements
	Version           uint32
}

func (j *JobSchedulingInfo) DeepCopy() *JobSchedulingInfo {
	return &JobSchedulingInfo{
		Lifetime:          j.Lifetime,
		PriorityClassName: j.PriorityClassName,
		SubmitTime:        j.SubmitTime,
		Priority:          j.Priority,
		PodRequirements:   j.PodRequirements,
		Version:           j.Version,
	}
}

// PodRequirements captures the scheduling requirements specific to a pod.
type PodRequirements struct {
	NodeSelector         map[string]string
	Affinity             *corev1.Affinity
	Tolerations          []corev1.Toleration
	Annotations          map[string]string
	ResourceRequirements corev1.ResourceRequirements
}

func (p *PodRequirements) DeepCopy() *PodRequirements {
	clonedResourceRequirements := proto.Clone(&p.ResourceRequirements).(*corev1.ResourceRequirements)
	return &PodRequirements{
		NodeSelector: maps.Clone(p.NodeSelector),
		Affinity:     proto.Clone(p.Affinity).(*corev1.Affinity),
		Annotations:  maps.Clone(p.Annotations),
		Tolerations: armadaslices.Map(p.Tolerations, func(t corev1.Toleration) corev1.Toleration {
			cloned := proto.Clone(&t).(*corev1.Toleration)
			return *cloned
		}),
		ResourceRequirements: *clonedResourceRequirements,
	}
}

func FromSchedulerObjectsJobSchedulingInfo(jobSchedulingInfo *schedulerobjects.JobSchedulingInfo) (*JobSchedulingInfo, error) {
	podRequirements := jobSchedulingInfo.GetPodRequirements()
	if podRequirements == nil {
		return nil, errors.Errorf("jobSchedulingInfo must have pod requirements")
	}

	return &JobSchedulingInfo{
		Lifetime:          jobSchedulingInfo.Lifetime,
		PriorityClassName: jobSchedulingInfo.PriorityClassName,
		SubmitTime:        jobSchedulingInfo.SubmitTime,
		Priority:          jobSchedulingInfo.Priority,
		PodRequirements: &PodRequirements{
			NodeSelector:         podRequirements.NodeSelector,
			Affinity:             podRequirements.Affinity,
			Tolerations:          podRequirements.Tolerations,
			Annotations:          podRequirements.Annotations,
			ResourceRequirements: podRequirements.ResourceRequirements,
		},
		Version: jobSchedulingInfo.Version,
	}, nil
}
