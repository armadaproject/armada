package internaltypes

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"

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
		PodRequirements:   j.PodRequirements.DeepCopy(),
		Version:           j.Version,
	}
}

// PodRequirements captures the scheduling requirements specific to a pod.
type PodRequirements struct {
	NodeSelector         map[string]string
	Affinity             *v1.Affinity
	Tolerations          []v1.Toleration
	Annotations          map[string]string
	ResourceRequirements v1.ResourceRequirements
}

func (p *PodRequirements) GetAffinityNodeSelector() *v1.NodeSelector {
	affinity := p.Affinity
	if affinity == nil {
		return nil
	}
	nodeAffinity := affinity.NodeAffinity
	if nodeAffinity == nil {
		return nil
	}
	return nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
}

func (p *PodRequirements) DeepCopy() *PodRequirements {
	clonedResourceRequirements := proto.Clone(&p.ResourceRequirements).(*v1.ResourceRequirements)
	return &PodRequirements{
		NodeSelector: maps.Clone(p.NodeSelector),
		Affinity:     proto.Clone(p.Affinity).(*v1.Affinity),
		Annotations:  maps.Clone(p.Annotations),
		Tolerations: armadaslices.Map(p.Tolerations, func(t v1.Toleration) v1.Toleration {
			cloned := proto.Clone(&t).(*v1.Toleration)
			return *cloned
		}),
		ResourceRequirements: *clonedResourceRequirements,
	}
}

func FromSchedulerObjectsJobSchedulingInfo(j *schedulerobjects.JobSchedulingInfo) (*JobSchedulingInfo, error) {
	podRequirements := j.GetPodRequirements()
	if podRequirements == nil {
		return nil, errors.Errorf("job must have pod requirements")
	}
	return &JobSchedulingInfo{
		Lifetime:          j.Lifetime,
		PriorityClassName: j.PriorityClassName,
		SubmitTime:        j.SubmitTime,
		Priority:          j.Priority,
		PodRequirements: &PodRequirements{
			NodeSelector:         podRequirements.NodeSelector,
			Affinity:             podRequirements.Affinity,
			Tolerations:          podRequirements.Tolerations,
			Annotations:          podRequirements.Annotations,
			ResourceRequirements: podRequirements.ResourceRequirements,
		},
		Version: j.Version,
	}, nil
}

func ToSchedulerObjectsJobSchedulingInfo(j *JobSchedulingInfo) *schedulerobjects.JobSchedulingInfo {
	podRequirements := j.PodRequirements
	return &schedulerobjects.JobSchedulingInfo{
		Lifetime:          j.Lifetime,
		PriorityClassName: j.PriorityClassName,
		SubmitTime:        j.SubmitTime,
		Priority:          j.Priority,
		ObjectRequirements: []*schedulerobjects.ObjectRequirements{
			{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: &schedulerobjects.PodRequirements{
						NodeSelector:         podRequirements.NodeSelector,
						Affinity:             podRequirements.Affinity,
						Tolerations:          podRequirements.Tolerations,
						Annotations:          podRequirements.Annotations,
						ResourceRequirements: podRequirements.ResourceRequirements,
					},
				},
			},
		},
		Version: j.Version,
	}
}
