package internaltypes

import (
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

// JobSchedulingInfo is a minimal representation of job requirements that the scheduler uses for scheduling
type JobSchedulingInfo struct {
	Lifetime        uint32
	PriorityClass   string
	SubmitTime      time.Time
	Priority        uint32
	PodRequirements *PodRequirements
	Version         uint32
	GangInfo        *api.Gang
}

func (j *JobSchedulingInfo) DeepCopy() *JobSchedulingInfo {
	var gangCopy *api.Gang
	if j.GangInfo != nil {
		gangCopy = &api.Gang{
			GangId:                  j.GangInfo.GangId,
			Cardinality:             j.GangInfo.Cardinality,
			NodeUniformityLabelName: j.GangInfo.NodeUniformityLabelName,
		}
	}
	return &JobSchedulingInfo{
		Lifetime:        j.Lifetime,
		PriorityClass:   j.PriorityClass,
		SubmitTime:      j.SubmitTime,
		Priority:        j.Priority,
		PodRequirements: j.PodRequirements.DeepCopy(),
		Version:         j.Version,
		GangInfo:        gangCopy,
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

// Gang returns the gang configuration
func (j *JobSchedulingInfo) Gang() *api.Gang {
	return j.GangInfo
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
	rr := podRequirements.GetResourceRequirements().DeepCopy()
	if rr == nil {
		rr = &v1.ResourceRequirements{}
	}

	// Extract Gang info - prefer structured Gang field from protobuf (new jobs)
	// Fall back to annotations only for backward compatibility (old jobs in database)
	var gangInfo *api.Gang
	if j.Gang != nil {
		// New style: Gang is stored in protobuf field, convert to api.Gang
		gangInfo = &api.Gang{
			GangId:                  j.Gang.GangId,
			Cardinality:             j.Gang.Cardinality,
			NodeUniformityLabelName: j.Gang.NodeUniformityLabelName,
		}
	} else if podRequirements.Annotations != nil {
		// Old style: Gang is stored in annotations (backward compatibility)
		gangId, hasGangId := podRequirements.Annotations["armadaproject.io/gangId"]
		gangCardStr, hasCardinality := podRequirements.Annotations["armadaproject.io/gangCardinality"]
		if hasGangId && hasCardinality {
			var cardinality uint32
			if parsed, err := strconv.ParseUint(gangCardStr, 10, 32); err == nil {
				cardinality = uint32(parsed)
				gangInfo = &api.Gang{
					GangId:      gangId,
					Cardinality: cardinality,
				}
				if nodeUniformity, ok := podRequirements.Annotations["armadaproject.io/gangNodeUniformityLabel"]; ok {
					gangInfo.NodeUniformityLabelName = nodeUniformity
				}
			}
		}
	} else {
	}

	return &JobSchedulingInfo{
		Lifetime:      j.Lifetime,
		PriorityClass: j.PriorityClassName,
		SubmitTime:    protoutil.ToStdTime(j.SubmitTime),
		Priority:      j.Priority,
		PodRequirements: &PodRequirements{
			NodeSelector: maps.Clone(podRequirements.NodeSelector),
			Affinity:     proto.Clone(podRequirements.Affinity).(*v1.Affinity),
			Tolerations: armadaslices.Map(podRequirements.Tolerations, func(t *v1.Toleration) v1.Toleration {
				cloned := proto.Clone(t).(*v1.Toleration)
				return *cloned
			}),
			Annotations:          maps.Clone(podRequirements.Annotations),
			ResourceRequirements: *rr,
		},
		Version:  j.Version,
		GangInfo: gangInfo,
	}, nil
}

func ToSchedulerObjectsJobSchedulingInfo(j *JobSchedulingInfo) *schedulerobjects.JobSchedulingInfo {
	podRequirements := j.PodRequirements

	// Convert Gang info back to schedulerobjects.Gang if present
	var gang *schedulerobjects.Gang
	if j.GangInfo != nil {
		gang = &schedulerobjects.Gang{
			GangId:                  j.GangInfo.GangId,
			Cardinality:             j.GangInfo.Cardinality,
			NodeUniformityLabelName: j.GangInfo.NodeUniformityLabelName,
			// NodeUniformityLabelValue is not stored in JobSchedulingInfo - it's ephemeral
		}
	}

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
							return proto.Clone(&t).(*v1.Toleration)
						}),
						Annotations:          maps.Clone(podRequirements.Annotations),
						ResourceRequirements: podRequirements.ResourceRequirements.DeepCopy(),
					},
				},
			},
		},
		Version: j.Version,
		Gang:    gang,
	}
}
