package resource

import (
	"math"
	"math/big"
	"sort"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// FromResourceList function takes a map with keys of type ResourceName and values of type
// "resource.Quantity" as defined in the K8s API.
//
// It converts the keys to strings and creates a new map with the same keys, but with deep copies of the values.
// The resulting map is of type map[string]resource.Quantity.
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

// String function handles the string representation  of ComputeResources i.e.
// how the output of the struct is represented when functions like fmt.Print()
// is called on it.
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

// Max function checks if the key  in the ComputeResources Map supplied as an argument, b exists
// and has a value greater than the already existing value mapped to the key in ComputeResources Map
// supplied as the method's target, "a", if so, the value of the key is replaced by b's value.
// If the key does not exist in "a", it is created and made to have a DeepCopy value of its value in b.
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

// Equal function compares a and b, returns false:
//   - if both are nil.
//   - if they have different lengths
//   - if a key exists in "a" but does not exist in "b" or vice versa.
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

// Dominates function compares two ComputeResources maps, a and b,
// to determine whether "a" is sufficient to meet or exceed the resource quantity of b
// if it does then "true" is returned if not "false" is returned.
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

// IsValid function checks if all the values in "a" is  greater than or equal to zero.
// It returns true if all values are valid i.e. all values are greater than or equal to zero,
// and false if any of the values are negative
func (a ComputeResources) IsValid() bool {
	valid := true
	for _, value := range a {
		valid = valid && value.Sign() >= 0
	}
	return valid
}

// LimitToZero function limits each value in "a" to a minimum value of zero.
// In the case any value in "a" has a value less than zero, it is replaced with a value of zero.
func (a ComputeResources) LimitToZero() {
	for key, value := range a {
		if value.Sign() < 0 {
			a[key] = *resource.NewQuantity(0, resource.BinarySI)
		}
	}
}

// IsZero function checks if every value in "a" is zero. If any value is not zero it returns false, if all are zero, it returns true.
func (a ComputeResources) IsZero() bool {
	if len(a) == 0 {
		return true
	}
	for _, value := range a {
		if !value.IsZero() {
			return false
		}
	}
	return true
}

// The Sub function subtracts the values in "a" from the values
// in "b". In the case a value exists in "b" but not in "a", the negative
// of the value is mapped to its key in "a". The Sub function can be visually
// represented as (a - b).
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

// The Mul function takes a ComputeResources object called "a" and multiplies each value in it with a given "factor".
// It then stores the result of each computation in a new ComputeResourcesFloat object,
// where each key in "a" maps to its corresponding value converted to a float64 type and multiplied by "factor"
func (a ComputeResources) Mul(factor float64) ComputeResourcesFloat {
	targetComputeResource := make(ComputeResourcesFloat)
	for key, value := range a {
		targetComputeResource[key] = QuantityAsFloat64(value) * factor
	}
	return targetComputeResource
}

// MulByResource function takes a ComputeResources object called "a" and a map of factors,
// where each key in the factors map corresponds to a key in "a".
// It multiplies each value in "a" by its corresponding factor from the map, or by 1 if the factor does not exist in the map.
// The computed values are stored in a new ComputeResourcesFloat object, which is returned by the function.
// The purpose of this function is to scale the values in a ComputeResources object by the factors specified in the input map.
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

// AsFloat function, converts ComputeResources to ComputeResourcesFloat.
func (a ComputeResources) AsFloat() ComputeResourcesFloat {
	targetComputeResource := make(ComputeResourcesFloat)
	for key, value := range a {
		targetComputeResource[key] = QuantityAsFloat64(value)
	}
	return targetComputeResource
}

// ComputeResourcesFloat is float version of compute resource, prefer calculations with quantity where possible
type ComputeResourcesFloat map[string]float64

// IsValid function checks if all the values in "a" is  greater than or equal to zero.
// It returns true if all values are valid i.e. all values are greater than or equal to zero,
// and false if any of the values are negative
func (a ComputeResourcesFloat) IsValid() bool {
	valid := true
	for _, value := range a {
		valid = valid && value >= 0
	}
	return valid
}

// The Sub function subtracts the values in "a" from the values
// in "b". In the case a value exists in "b" but not in "a", the negative
// of the value is mapped to its key in "a". The Sub function can be visually
// represented as (a - b).
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

// The Max function maps every key in "a" with its maximum when compared with its value and its corresponding  value in "b".
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

// IsLessThan function checks if any value a contains any negative value after carrying out a
// subtraction operation of a - b using the "Sub" method. If there are any negative value in "a" after this
// operation, then "a" is less than "b", a boolean value, "true" is returned, otherwise
// the boolean value, "false" is returned.
func (a ComputeResourcesFloat) IsLessThan(b ComputeResourcesFloat) bool {
	reduced := a.DeepCopy()
	reduced.Sub(b)
	return !reduced.IsValid()
}

// LimitWith function limits the values of "a" to the corresponding values in the limit object.
// It creates a new ComputeResourcesFloat object with the same keys as the original "a" object,
// but with values limited by the corresponding values in the limit object using the math.Min() function.
// If any value in the "a" map is greater than its corresponding value in the limit map,
// then that value in "a" is replaced with the corresponding value from the limit map.
// This means that the limit map acts as a maximum limit on the values in the "a" map.
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

// LimitToZero function limits each value in "a" to a minimum value of zero.
// In the case any value in "a" has a value less than zero, it is replaced with a value of zero.
func (a ComputeResourcesFloat) LimitToZero() {
	for key, value := range a {
		a[key] = math.Max(value, 0)
	}
}

// The Mul function takes a ComputeResources object called "a" and multiplies each value in it with a given "factor".
// It then stores the result of each computation in a new ComputeResourcesFloat object,
// where each key in "a" maps to its corresponding value multiplied by "factor"
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
// So pod resource usage is the max for each resource type (cpu/memory etc.) that could be used at any given time
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

// TotalPodResourceLimit function calculates the maximum total resource (cpu, memory, etc.) limits in the pod for
// each resource by iterating through all containers and initContainers in the pod.
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

// CalculateTotalResource computes the combined total quantity of each resource (cpu, memory, etc) available for scheduling
// in the slice of nodes supplied as argument in the function.
func CalculateTotalResource(nodes []*v1.Node) ComputeResources {
	totalResources := make(ComputeResources)
	for _, node := range nodes {
		nodeAllocatableResource := FromResourceList(node.Status.Allocatable)
		totalResources.Add(nodeAllocatableResource)
	}
	return totalResources
}

// CalculateTotalResourceRequest computes the combined total quantity of each resource (cpu, memory, etc) requested by each pod
// in the slice of pods supplied as argument in the function.
func CalculateTotalResourceRequest(pods []*v1.Pod) ComputeResources {
	totalResources := make(ComputeResources)
	for _, pod := range pods {
		podResource := TotalPodResourceRequest(&pod.Spec)
		totalResources.Add(podResource)
	}
	return totalResources
}
