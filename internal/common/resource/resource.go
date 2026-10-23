package resource

import (
	"sort"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	resourcehelper "k8s.io/component-helpers/resource"
)

// PodsResourceName is the Kubernetes resource key for per-node pod capacity
// (node.Status.Allocatable["pods"]), used by the scheduler's RespectNodePodLimits feature.
const PodsResourceName = "pods"

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

func FromProtoMap(m map[string]*resource.Quantity) ComputeResources {
	resources := make(ComputeResources, len(m))
	for k, v := range m {
		if v != nil {
			resources[k] = *v
		}
	}
	return resources
}

func (a ComputeResources) ToProtoMap() map[string]*resource.Quantity {
	resources := make(map[string]*resource.Quantity, len(a))
	for k, v := range a {
		r := v.DeepCopy()
		resources[k] = &r
	}
	return resources
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

// Zero zeroes out rl such that all quantities have value 0.
func (a ComputeResources) Zero() {
	for key := range a {
		a[key] = *resource.NewQuantity(0, resource.BinarySI)
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

func (a ComputeResources) Div(b map[string]int64) {
	for k, v := range b {
		existing, ok := a[k]
		if ok {
			if existing.Format == resource.DecimalSI {
				a[k] = resource.NewMilliQuantity(existing.MilliValue()/v, existing.Format).DeepCopy()
			} else {
				a[k] = resource.NewQuantity(existing.Value()/v, existing.Format).DeepCopy()
			}
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

func (a ComputeResources) AsKubernetesResourceList() v1.ResourceList {
	kubernetesResourceList := map[v1.ResourceName]resource.Quantity{}

	for name, quantity := range a {
		kubernetesResourceList[v1.ResourceName(name)] = quantity
	}

	return kubernetesResourceList
}

// ComputeResourcesFloat is float version of compute resource, prefer calculations with quantity where possible
type ComputeResourcesFloat map[string]float64

// podResourcesOptions leaves out the pod overhead. The RuntimeClass of the executor cluster adds the overhead.
// The scheduler does not know that overhead. The executor also leaves it out, so both totals stay equal.
var podResourcesOptions = resourcehelper.PodResourcesOptions{ExcludeOverhead: true}

// PodResourceRequirements returns the resource requests and limits of a pod as Kubernetes computes them
// for scheduling and admission. The result includes native sidecars, classic init containers, and pod-level resources.
func PodResourceRequirements(podSpec *v1.PodSpec) *v1.ResourceRequirements {
	pod := &v1.Pod{Spec: *podSpec}
	requests := resourcehelper.PodRequests(pod, podResourcesOptions)
	limits := resourcehelper.PodLimits(pod, podResourcesOptions)
	dropUndeclaredZeroResources(requests, podSpec, func(r v1.ResourceRequirements) v1.ResourceList { return r.Requests })
	dropUndeclaredZeroResources(limits, podSpec, func(r v1.ResourceRequirements) v1.ResourceList { return r.Limits })
	// The helper results share the pod-level quantities with the pod spec. The copies keep callers from changing the spec.
	return &v1.ResourceRequirements{Requests: requests.DeepCopy(), Limits: limits.DeepCopy()}
}

// TotalPodResourceRequest returns the requests of PodResourceRequirements as ComputeResources.
// It keeps zero-valued resources. The executor does not check resource names, so a zero value has no effect there.
func TotalPodResourceRequest(podSpec *v1.PodSpec) ComputeResources {
	requests := resourcehelper.PodRequests(&v1.Pod{Spec: *podSpec}, podResourcesOptions)
	// FromResourceList copies each quantity, so the result shares no quantity with the pod spec.
	return FromResourceList(requests)
}

// dropUndeclaredZeroResources removes a zero-valued resource that no main container and no native sidecar
// declares. The scheduler rejects a job that requests a resource it does not support, even at zero. A zero value from
// a classic init container or from the pod-level block has no effect on scheduling.
func dropUndeclaredZeroResources(
	total v1.ResourceList,
	podSpec *v1.PodSpec,
	resourcesOf func(v1.ResourceRequirements) v1.ResourceList,
) {
	declared := map[v1.ResourceName]bool{}
	for _, c := range podSpec.Containers {
		for name := range resourcesOf(c.Resources) {
			declared[name] = true
		}
	}
	for _, c := range podSpec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == v1.ContainerRestartPolicyAlways {
			for name := range resourcesOf(c.Resources) {
				declared[name] = true
			}
		}
	}
	for name, quantity := range total {
		if quantity.IsZero() && !declared[name] {
			delete(total, name)
		}
	}
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
