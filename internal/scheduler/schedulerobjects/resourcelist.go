package schedulerobjects

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	cr "github.com/armadaproject/armada/internal/common/resource"
)

func V1ResourceListFromResourceList(rl *ResourceList) v1.ResourceList {
	rv := make(v1.ResourceList, len(rl.Resources))
	for t, q := range rl.Resources {
		rv[v1.ResourceName(t)] = q.DeepCopy()
	}
	return rv
}

type QuantityByTAndResourceType[T comparable] map[T]*ResourceList

func (rl *ResourceList) Get(resourceType string) resource.Quantity {
	qty := rl.Resources[resourceType]
	if qty == nil {
		return resource.Quantity{}
	}
	return *qty
}

func (rl *ResourceList) Set(t string, q resource.Quantity) {
	cpy := q.DeepCopy()
	rl.initialise()
	rl.Resources[t] = &cpy
}

func (a *ResourceList) Sub(b *ResourceList) {
	a.initialise()
	for t, qb := range b.Resources {
		qa := a.Resources[t]
		qa.Sub(*qb)
		a.Resources[t] = qa
	}
}

func (rl *ResourceList) DeepCopy() *ResourceList {
	if len(rl.Resources) == 0 {
		return &ResourceList{}
	}
	rv := ResourceList{
		Resources: make(map[string]*resource.Quantity, len(rl.Resources)),
	}
	for t, q := range rl.Resources {
		cpy := q.DeepCopy()
		rv.Resources[t] = &cpy
	}
	return &rv
}

func (a *ResourceList) Equal(b *ResourceList) bool {
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
		rl.Resources = make(map[string]*resource.Quantity)
	}
}

func (rl ResourceList) ToComputeResources() cr.ComputeResources {
	rv := make(cr.ComputeResources, len(rl.Resources))
	for k, v := range rl.Resources {
		rv[k] = v.DeepCopy()
	}
	return rv
}

// AllocatableByPriorityAndResourceType accounts for resources that can be allocated to pods of a given priority.
// E.g., AllocatableByPriorityAndResourceType[5]["cpu"] is the amount of CPU available to pods with priority 5,
// where alloctable resources = unused resources + resources allocated to lower-priority pods.
type AllocatableByPriorityAndResourceType QuantityByTAndResourceType[int32]

func NewAllocatableByPriorityAndResourceType(priorities []int32, rl *ResourceList) AllocatableByPriorityAndResourceType {
	rv := make(AllocatableByPriorityAndResourceType)
	for _, priority := range priorities {
		rv[priority] = rl.DeepCopy()
	}
	return rv
}

// MarkAllocated indicates resources have been allocated to pods of priority p,
// hence reducing the resources allocatable to pods of priority p or lower.
func (m AllocatableByPriorityAndResourceType) MarkAllocated(p int32, rs *ResourceList) {
	for priority, allocatableResourcesAtPriority := range m {
		if priority <= p {
			allocatableResourcesAtPriority.Sub(rs)
		}
	}
}
