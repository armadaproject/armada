package common

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

type ComputeResources map[string]resource.Quantity

func FromResourceList(list v1.ResourceList) ComputeResources {
	resources := make(ComputeResources)
	for k, v := range list {
		resources[string(k)] = v.DeepCopy()
	}
	return resources
}

func (a ComputeResources) Add(b ComputeResources) {
	for k, v := range b {
		existing, ok := a[k]
		if ok {
			existing.Add(v)
		} else {
			a[k] = v.DeepCopy()
		}
	}
}

func (a ComputeResources) Sub(b ComputeResources) {
	for k, v := range b {
		existing, ok := a[k]
		if ok {
			existing.Sub(v)
		} else {
			a[k] = v.DeepCopy()
		}
	}
}

func (a ComputeResources) DeepCopy() ComputeResources {
	targetComputeResource := make(ComputeResources)

	for key, value := range a {
		targetComputeResource[key] = value.DeepCopy()
	}

	return targetComputeResource
}
