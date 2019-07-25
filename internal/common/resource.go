package common

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"math"
	"math/big"
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
			cpy := v.DeepCopy()
			cpy.Neg()
			a[k] = cpy
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

func (a ComputeResources) Mul(factor float64) ComputeResourcesFloat {
	targetComputeResource := make(ComputeResourcesFloat)
	for key, value := range a {
		targetComputeResource[key] = QuantityAsFloat64(value) * factor
	}
	return targetComputeResource
}

func (a ComputeResources) AsFloat() ComputeResourcesFloat {
	targetComputeResource := make(ComputeResourcesFloat)
	for key, value := range a {
		targetComputeResource[key] = QuantityAsFloat64(value)
	}
	return targetComputeResource
}

func QuantityAsFloat64(q resource.Quantity) float64 {
	dec := q.AsDec()
	unscaled := dec.UnscaledBig()
	scale := dec.Scale()
	unscaledFloat, _ := new(big.Float).SetInt(unscaled).Float64()
	return unscaledFloat * math.Pow10(-int(scale))
}

// float version of compute resource, prefer calculations with quantity where possible
type ComputeResourcesFloat map[string]float64

func (a ComputeResourcesFloat) IsValid() bool {
	valid := true
	for _, value := range a {
		valid = valid && value >= 0
	}
	return valid;
}

func (a ComputeResourcesFloat) Sub(b ComputeResourcesFloat) {
	for k, v := range b {
		existing, ok := a[k]
		if ok {
			a[k] = existing - v
		} else {
			a[k] = - v
		}
	}
}

func (a ComputeResourcesFloat) DeepCopy() ComputeResourcesFloat {
	targetComputeResource := make(ComputeResourcesFloat)
	for key, value := range a {
		targetComputeResource[key] = value
	}
	return targetComputeResource
}

func TotalResourceLimit(podSpec *v1.PodSpec) ComputeResources {
	totalResources := make(ComputeResources)
	for _, container := range podSpec.Containers {
		containerResourceLimit := FromResourceList(container.Resources.Limits)
		totalResources.Add(containerResourceLimit)
	}
	return totalResources
}
