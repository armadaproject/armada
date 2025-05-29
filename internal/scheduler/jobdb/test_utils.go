package jobdb

import (
	"k8s.io/apimachinery/pkg/api/resource"

	schedulerconfiguration "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

var testResourceListFactory = makeTestResourceListFactory()

func makeTestResourceListFactory() *internaltypes.ResourceListFactory {
	result, _ := internaltypes.NewResourceListFactory(
		getTestSupportedResourceTypes(),
		getTestFloatingResourceTypes(),
	)
	return result
}

func getTestSupportedResourceTypes() []schedulerconfiguration.ResourceType {
	return []schedulerconfiguration.ResourceType{
		{Name: "memory", Resolution: resource.MustParse("1")},
		{Name: "cpu", Resolution: resource.MustParse("1m")},
		{Name: "nvidia.com/gpu", Resolution: resource.MustParse("1m")},
		{Name: "foo", Resolution: resource.MustParse("1m")},
	}
}

func getTestFloatingResourceTypes() []schedulerconfiguration.FloatingResourceConfig {
	return []schedulerconfiguration.FloatingResourceConfig{
		{Name: "storage-connections", Resolution: resource.MustParse("1")},
	}
}

func JobWithJobSchedulingInfo(job *Job, jobSchedulingInfo *internaltypes.JobSchedulingInfo) *Job {
	j, err := job.WithJobSchedulingInfo(jobSchedulingInfo)
	if err != nil {
		panic(err)
	}
	return j
}
