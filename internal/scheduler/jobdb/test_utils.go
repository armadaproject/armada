package jobdb

import (
	"k8s.io/apimachinery/pkg/api/resource"

	schedulerconfiguration "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

var TestResourceListFactory = MakeTestResourceListFactory()

func MakeTestResourceListFactory() *internaltypes.ResourceListFactory {
	result, _ := internaltypes.MakeResourceListFactory(GetTestSupportedResourceTypes())
	return result
}

func GetTestSupportedResourceTypes() []schedulerconfiguration.ResourceType {
	return []schedulerconfiguration.ResourceType{
		{Name: "memory", Resolution: resource.MustParse("1")},
		{Name: "cpu", Resolution: resource.MustParse("1m")},
		{Name: "gpu", Resolution: resource.MustParse("1m")},
		{Name: "foo", Resolution: resource.MustParse("1m")},
	}
}

func WithJobDbJobPodRequirements(job *Job, reqs *schedulerobjects.PodRequirements) *Job {
	return JobWithJobSchedulingInfo(job, &schedulerobjects.JobSchedulingInfo{
		PriorityClassName: job.JobSchedulingInfo().PriorityClassName,
		SubmitTime:        job.JobSchedulingInfo().SubmitTime,
		ObjectRequirements: []*schedulerobjects.ObjectRequirements{
			{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: reqs,
				},
			},
		},
	})
}

func JobWithJobSchedulingInfo(job *Job, jobSchedulingInfo *schedulerobjects.JobSchedulingInfo) *Job {
	j, err := job.WithJobSchedulingInfo(jobSchedulingInfo)
	if err != nil {
		panic(err)
	}
	return j
}
