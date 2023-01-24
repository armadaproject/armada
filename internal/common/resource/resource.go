package resource

import (
	"math"
	"math/big"
	"sort"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func FromResourceList(list v1.ResourceList) ComputeResources {
	resources := make(ComputeResources)
	for k, v := range list {
		resources[string(k)] = v.DeepCopy()
	}
	return resources
}

// QuantityAsFloat64 returns a float64 representation of a quantity.
// We need our own function because q.AsApproximateFloat64 sometimes returns surprising results.
// For example, resource.MustParse("5188205838208Ki").AsApproximateFloat64() returns 0.004291583283300088,
// whereas this function returns 5.312722778324993e+15.
func QuantityAsFloat64(q resource.Quantity) float64 {
	dec := q.AsDec()
	unscaled := dec.UnscaledBig()
	scale := dec.Scale()
	unscaledFloat, _ := new(big.Float).SetInt(unscaled).Float64()
	return unscaledFloat * math.Pow10(-int(scale))
}

type ComputeResources map[string]resource.Quantity

func (a ComputeResources) String() string {
	str := ""

	keys := make([]string, 0, len(a))
	for k := range a {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if str != "" {
			str += ", "
		}
		v := a[k]
		str += k + ": " + v.String()
	}
	return str
}

func (a ComputeResources) Add(b ComputeResources) {
	for k, v := range b {
		existing, ok := a[k]
		if ok {
			existing.Add(v)
			a[k] = existing
		} else {
			a[k] = v.DeepCopy()
		}
	}
}

func (a ComputeResources) Max(b ComputeResources) {
	for k, v := range b {
		existing, ok := a[k]
		if ok {
			if v.Cmp(existing) > 0 {
				a[k] = v.DeepCopy()
			}
		} else {
			a[k] = v.DeepCopy()
		}
	}
}

func (a ComputeResources) Equal(b ComputeResources) bool {
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range b {
		existing, ok := a[k]
		if !ok {
			return false
		}
		if !existing.Equal(v) {
			return false
		}
	}
	return true
}

func (a ComputeResources) Dominates(b ComputeResources) bool {
	reduced := a.DeepCopy()
	reduced.Sub(b)

	hasRemainder := false
	for _, value := range reduced {
		if value.Sign() < 0 {
			return false
		}
		if value.Sign() > 0 {
			hasRemainder = true
		}
	}

	return hasRemainder
}

func (a ComputeResources) IsValid() bool {
	valid := true
	for _, value := range a {
		valid = valid && value.Sign() >= 0
	}
	return valid
}

func (a ComputeResources) Sub(b ComputeResources) {
	if b == nil {
		return
	}
	for k, v := range b {
		existing, ok := a[k]
		if ok {
			existing.Sub(v)
			a[k] = existing
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

func (a ComputeResources) MulByResource(factors map[string]float64) ComputeResourcesFloat {
	targetComputeResource := make(ComputeResourcesFloat)
	for key, value := range a {
		factor, exists := factors[key]
		if !exists {
			factor = 1
		}
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

// ComputeResourcesFloat is float version of compute resource, prefer calculations with quantity where possible
type ComputeResourcesFloat map[string]float64

func (a ComputeResourcesFloat) IsValid() bool {
	valid := true
	for _, value := range a {
		valid = valid && value >= 0
	}
	return valid
}

func (a ComputeResourcesFloat) Sub(b ComputeResourcesFloat) {
	if b == nil {
		return
	}
	for k, v := range b {
		existing, ok := a[k]
		if ok {
			a[k] = existing - v
		} else {
			a[k] = -v
		}
	}
}

func (a ComputeResourcesFloat) Add(b ComputeResourcesFloat) {
	if b == nil {
		return
	}
	for k, v := range b {
		existing, ok := a[k]
		if ok {
			a[k] = existing + v
		} else {
			a[k] = v
		}
	}
}

func (a ComputeResourcesFloat) Max(b ComputeResourcesFloat) {
	for k, v := range b {
		existing, ok := a[k]
		if ok {
			a[k] = math.Max(v, existing)
		} else {
			a[k] = v
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

func (a ComputeResourcesFloat) IsLessThan(b ComputeResourcesFloat) bool {
	reduced := a.DeepCopy()
	reduced.Sub(b)
	return !reduced.IsValid()
}

func (a ComputeResourcesFloat) LimitWith(limit ComputeResourcesFloat) ComputeResourcesFloat {
	targetComputeResource := make(ComputeResourcesFloat)
	for key, value := range a {
		targetComputeResource[key] = math.Min(value, limit[key])
	}
	return targetComputeResource
}

// MergeWith represents the merged in values take precedence and override existing values for the same key
func (a ComputeResourcesFloat) MergeWith(merged ComputeResourcesFloat) ComputeResourcesFloat {
	targetComputeResource := a.DeepCopy()
	for key, value := range merged {
		targetComputeResource[key] = value
	}
	return targetComputeResource
}

func (a ComputeResourcesFloat) LimitToZero() {
	for key, value := range a {
		a[key] = math.Max(value, 0)
	}
}

func (a ComputeResourcesFloat) Mul(factor float64) ComputeResourcesFloat {
	targetComputeResource := make(ComputeResourcesFloat)
	for key, value := range a {
		targetComputeResource[key] = value * factor
	}
	return targetComputeResource
}

// TotalPodResourceRequest represents the resource request for a given pod is the maximum of:
//   - sum of all containers
//   - any individual init container
//
// This is because:
//   - containers run in parallel (so need to sum resources)
//   - init containers run sequentially (so only their individual resource need be considered)
//
// So pod resource usage is the max for each resource type (cpu/memory etc) that could be used at any given time
func TotalPodResourceRequest(podSpec *v1.PodSpec) ComputeResources {
	totalResources := make(ComputeResources)
	for _, container := range podSpec.Containers {
		containerResource := FromResourceList(container.Resources.Requests)
		totalResources.Add(containerResource)
	}

	for _, initContainer := range podSpec.InitContainers {
		containerResource := FromResourceList(initContainer.Resources.Requests)
		totalResources.Max(containerResource)
	}
	return totalResources
}

func TotalPodResourceLimit(podSpec *v1.PodSpec) ComputeResources {
	totalResources := make(ComputeResources)
	for _, container := range podSpec.Containers {
		containerResource := FromResourceList(container.Resources.Limits)
		totalResources.Add(containerResource)
	}

	for _, initContainer := range podSpec.InitContainers {
		containerResource := FromResourceList(initContainer.Resources.Limits)
		totalResources.Max(containerResource)
	}
	return totalResources
}

func CalculateTotalResource(nodes []*v1.Node) ComputeResources {
	totalResources := make(ComputeResources)
	for _, node := range nodes {
		nodeAllocatableResource := FromResourceList(node.Status.Allocatable)
		totalResources.Add(nodeAllocatableResource)
	}
	return totalResources
}

func CalculateTotalResourceRequest(pods []*v1.Pod) ComputeResources {
	totalResources := make(ComputeResources)
	for _, pod := range pods {
		podResource := TotalPodResourceRequest(&pod.Spec)
		totalResources.Add(podResource)
	}
	return totalResources
}
