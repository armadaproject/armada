package internaltypes

import (
	"time"

	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// JobSchedulingInfo is a minimal representation of job requirements that the scheduler uses for scheduling
type JobSchedulingInfo struct {
	Lifetime        uint32
	PriorityClass   string
	SubmitTime      time.Time
	Priority        uint32
	PodRequirements *PodRequirements
	Version         uint32
}

func (j *JobSchedulingInfo) DeepCopy() *JobSchedulingInfo {
	return &JobSchedulingInfo{
		Lifetime:        j.Lifetime,
		PriorityClass:   j.PriorityClass,
		SubmitTime:      j.SubmitTime,
		Priority:        j.Priority,
		PodRequirements: j.PodRequirements.DeepCopy(),
		Version:         j.Version,
	}
}

func (j *JobSchedulingInfo) Annotations() map[string]string {
	if j.PodRequirements != nil {
		return j.PodRequirements.Annotations
	}
	return map[string]string{}
}

func (j *JobSchedulingInfo) PriorityClassName() string {
	return j.PriorityClass
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
	clonedResourceRequirements := p.ResourceRequirements.DeepCopy()
	return &PodRequirements{
		NodeSelector: maps.Clone(p.NodeSelector),
		Affinity:     p.Affinity.DeepCopy(),
		Annotations:  maps.Clone(p.Annotations),
		Tolerations: armadaslices.Map(p.Tolerations, func(t v1.Toleration) v1.Toleration {
			cloned := t.DeepCopy()
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
	rr := podRequirements.GetResourceRequirements().DeepCopy()
	if rr == nil {
		rr = &v1.ResourceRequirements{}
	}
	return &JobSchedulingInfo{
		Lifetime:      j.Lifetime,
		PriorityClass: j.PriorityClassName,
		SubmitTime:    protoutil.ToStdTime(j.SubmitTime),
		Priority:      j.Priority,
		PodRequirements: &PodRequirements{
			NodeSelector: maps.Clone(podRequirements.NodeSelector),
			Affinity:     podRequirements.Affinity.DeepCopy(),
			Tolerations: armadaslices.Map(podRequirements.Tolerations, func(t *v1.Toleration) v1.Toleration {
				cloned := t.DeepCopy()
				return *cloned
			}),
			Annotations:          maps.Clone(podRequirements.Annotations),
			ResourceRequirements: *rr,
		},
		Version: j.Version,
	}, nil
}

func ToSchedulerObjectsJobSchedulingInfo(j *JobSchedulingInfo) *schedulerobjects.JobSchedulingInfo {
	podRequirements := j.PodRequirements
	return &schedulerobjects.JobSchedulingInfo{
		Lifetime:          j.Lifetime,
		PriorityClassName: j.PriorityClassName(),
		SubmitTime:        protoutil.ToTimestamp(j.SubmitTime),
		Priority:          j.Priority,
		ObjectRequirements: []*schedulerobjects.ObjectRequirements{
			{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: &schedulerobjects.PodRequirements{
						NodeSelector: maps.Clone(podRequirements.NodeSelector),
						Affinity:     podRequirements.Affinity.DeepCopy(),
						Tolerations: armadaslices.Map(podRequirements.Tolerations, func(t v1.Toleration) *v1.Toleration {
							return t.DeepCopy()
						}),
						Annotations:          maps.Clone(podRequirements.Annotations),
						ResourceRequirements: podRequirements.ResourceRequirements.DeepCopy(),
					},
				},
			},
		},
		Version: j.Version,
	}
}
