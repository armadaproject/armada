package schedulerobjects

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Most jobs specify 3 or fewer resources. We add 1 extra for margin.
const resourceListDefaultSize = 4

// NewResourceList returns a new ResourceList, where the backing map has initial capacity n.
func NewResourceList(n int) ResourceList {
	return ResourceList{Resources: make(map[string]resource.Quantity, n)}
}

// NewResourceListWithDefaultSize returns a new ResourceList, where the backing map has default initial capacity.
func NewResourceListWithDefaultSize() ResourceList {
	return ResourceList{Resources: make(map[string]resource.Quantity, resourceListDefaultSize)}
}

func ResourceListFromV1ResourceList(rl v1.ResourceList) ResourceList {
	rv := ResourceList{
		Resources: make(map[string]resource.Quantity, len(rl)),
	}
	for t, q := range rl {
		rv.Resources[string(t)] = q
	}
	return rv
}

func V1ResourceListFromResourceList(rl ResourceList) v1.ResourceList {
	rv := make(v1.ResourceList, len(rl.Resources))
	for t, q := range rl.Resources {
		rv[v1.ResourceName(t)] = q.DeepCopy()
	}
	return rv
}

type QuantityByTAndResourceType[T comparable] map[T]ResourceList

func (a QuantityByTAndResourceType[T]) AddResourceList(t T, rlb ResourceList) {
	rla := a[t]
	rla.Add(rlb)
	a[t] = rla
}

func (a QuantityByTAndResourceType[T]) AggregateByResource() ResourceList {
	rv := NewResourceListWithDefaultSize()
	for _, rl := range a {
		rv.Add(rl)
	}
	return rv
}

// MaxAggregatedByResource updates a in-place such that for each resource type t
// a[p1][t] + ... + a[pn][t] = max(a[p1][t] + ... + a[pn][t], rl[t]),
// where p1, ..., pn are the priorities in a, for each resource set explicitly in rl.
//
// If necessary to add resources to make up the difference, those resources are added at priority p.
func (a QuantityByTAndResourceType[T]) MaxAggregatedByResource(t T, rl ResourceList) {
	aggregate := a.AggregateByResource()
	var difference ResourceList
	for t, q := range rl.Resources {
		q = q.DeepCopy()
		q.Sub(aggregate.Get(t))
		if q.Cmp(resource.Quantity{}) == 1 {
			difference.AddQuantity(t, q)
		}
	}
	if len(difference.Resources) > 0 {
		a.AddResourceList(t, difference)
	}
}

func (rl *ResourceList) Get(resourceType string) resource.Quantity {
	return rl.Resources[resourceType]
}

func (rl *ResourceList) Set(t string, q resource.Quantity) {
	rl.initialise()
	rl.Resources[t] = q
}

func (a *ResourceList) Add(b ResourceList) {
	a.initialise()
	for t, qb := range b.Resources {
		qa := a.Resources[t]
		qa.Add(qb)
		a.Resources[t] = qa
	}
}

func (a *ResourceList) AddV1ResourceList(b v1.ResourceList) {
	a.initialise()
	for t, qb := range b {
		qa := a.Resources[string(t)]
		qa.Add(qb)
		a.Resources[string(t)] = qa
	}
}

func (a *ResourceList) SubV1ResourceList(b v1.ResourceList) {
	a.initialise()
	for t, qb := range b {
		qa := a.Resources[string(t)]
		qa.Sub(qb)
		a.Resources[string(t)] = qa
	}
}

func (rl *ResourceList) AddQuantity(resourceType string, quantity resource.Quantity) {
	rl.initialise()
	q := rl.Resources[resourceType]
	q.Add(quantity)
	rl.Resources[resourceType] = q
}

func (a *ResourceList) Sub(b ResourceList) {
	a.initialise()
	for t, qb := range b.Resources {
		qa := a.Resources[t]
		qa.Sub(qb)
		a.Resources[t] = qa
	}
}

func (rl ResourceList) DeepCopy() ResourceList {
	if len(rl.Resources) == 0 {
		return ResourceList{}
	}
	rv := ResourceList{
		Resources: make(map[string]resource.Quantity, len(rl.Resources)),
	}
	for t, q := range rl.Resources {
		rv.Resources[t] = q.DeepCopy()
	}
	return rv
}

// Zero zeroes out rl in-place, such that all quantities have value 0.
func (rl ResourceList) Zero() {
	for t, q := range rl.Resources {
		q.Set(0)
		rl.Resources[t] = q
	}
}

func (a ResourceList) Equal(b ResourceList) bool {
	for t, qa := range a.Resources {
		if qa.Cmp(b.Get(t)) != 0 {
			return false
		}
	}
	for t, qb := range b.Resources {
		if qb.Cmp(a.Get(t)) != 0 {
			return false
		}
	}
	return true
}

func (rl *ResourceList) initialise() {
	if rl.Resources == nil {
		rl.Resources = make(map[string]resource.Quantity)
	}
}

// AllocatableByPriorityAndResourceType accounts for resources that can be allocated to pods of a given priority.
// E.g., AllocatableByPriorityAndResourceType[5]["cpu"] is the amount of CPU available to pods with priority 5,
// where alloctable resources = unused resources + resources allocated to lower-priority pods.
type AllocatableByPriorityAndResourceType QuantityByTAndResourceType[int32]

func NewAllocatableByPriorityAndResourceType(priorities []int32, rl ResourceList) AllocatableByPriorityAndResourceType {
	rv := make(AllocatableByPriorityAndResourceType)
	for _, priority := range priorities {
		rv[priority] = rl.DeepCopy()
	}
	return rv
}

// MarkAllocated indicates resources have been allocated to pods of priority p,
// hence reducing the resources allocatable to pods of priority p or lower.
func (m AllocatableByPriorityAndResourceType) MarkAllocated(p int32, rs ResourceList) {
	for priority, allocatableResourcesAtPriority := range m {
		if priority <= p {
			allocatableResourcesAtPriority.Sub(rs)
		}
	}
}
