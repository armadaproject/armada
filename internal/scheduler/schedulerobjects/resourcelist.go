package schedulerobjects

import (
	"fmt"
	"strings"

	resource "k8s.io/apimachinery/pkg/api/resource"
)

type QuantityByPriorityAndResourceType map[int32]ResourceList

func (a QuantityByPriorityAndResourceType) DeepCopy() QuantityByPriorityAndResourceType {
	rv := make(QuantityByPriorityAndResourceType)
	for p, rl := range a {
		rv[p] = rl.DeepCopy()
	}
	return rv
}

func (a QuantityByPriorityAndResourceType) String() string {
	var sb strings.Builder
	i := 0
	sb.WriteString("{")
	for p, rl := range a {
		if i < len(a)-1 {
			sb.WriteString(fmt.Sprintf("%d: %s, ", p, rl.CompactString()))
		} else {
			sb.WriteString(fmt.Sprintf("%d: %s", p, rl.CompactString()))
		}
	}
	sb.WriteString("}")
	return sb.String()
}

func (a QuantityByPriorityAndResourceType) Add(b QuantityByPriorityAndResourceType) {
	for p, rsb := range b {
		rsa := a[p]
		rsa.Add(rsb)
		a[p] = rsa
	}
}

func (a QuantityByPriorityAndResourceType) Sub(b QuantityByPriorityAndResourceType) {
	for p, rsb := range b {
		rsa := a[p]
		rsa.Sub(rsb)
	}
}

func (a QuantityByPriorityAndResourceType) Equal(b QuantityByPriorityAndResourceType) bool {
	if len(a) != len(b) {
		return false
	}
	if a == nil {
		if b == nil {
			return true
		} else {
			return false
		}
	}
	if b == nil && a != nil {
		return false
	}
	for p, rla := range a {
		if rlb, ok := b[p]; ok {
			if !rla.Equal(rlb) {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

func (a QuantityByPriorityAndResourceType) AggregateByResource() ResourceList {
	rv := ResourceList{
		Resources: make(map[string]resource.Quantity),
	}
	for _, rl := range a {
		rv.Add(rl)
	}
	return rv
}

func (a *ResourceList) Get(resourceType string) resource.Quantity {
	if a.Resources == nil {
		return resource.Quantity{}
	}
	return a.Resources[resourceType]
}

func (a *ResourceList) Add(b ResourceList) {
	if a.Resources == nil {
		a.Resources = make(map[string]resource.Quantity)
	}
	for t, qb := range b.Resources {
		qa := a.Resources[t]
		qa.Add(qb)
		a.Resources[t] = qa
	}
}

func (a *ResourceList) Sub(b ResourceList) {
	if a.Resources == nil {
		a.Resources = make(map[string]resource.Quantity)
	}
	for t, qb := range b.Resources {
		qa := a.Resources[t]
		qa.Sub(qb)
		a.Resources[t] = qa
	}
}

func (rl *ResourceList) DeepCopy() ResourceList {
	rv := ResourceList{
		Resources: make(map[string]resource.Quantity),
	}
	for t, q := range rl.Resources {
		rv.Resources[t] = q.DeepCopy()
	}
	return rv
}

func (a ResourceList) Equal(b ResourceList) bool {
	if len(a.Resources) != len(b.Resources) {
		return false
	}
	if a.Resources == nil {
		if b.Resources == nil {
			return true
		} else {
			return false
		}
	}
	if b.Resources == nil && a.Resources != nil {
		return false
	}
	for t, qa := range a.Resources {
		if qb, ok := b.Resources[t]; ok {
			if qa.Cmp(qb) != 0 {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

func (rl ResourceList) CompactString() string {
	var sb strings.Builder
	sb.WriteString("{")
	i := 0
	for t, q := range rl.Resources {
		if i < len(rl.Resources)-1 {
			sb.WriteString(fmt.Sprintf("%s: %s, ", t, q.String()))
		} else {
			sb.WriteString(fmt.Sprintf("%s: %s", t, q.String()))
		}
		i++
	}
	sb.WriteString("}")
	return sb.String()
}

// AllocatableByPriorityAndResourceType accounts for resources that can be allocated to pods of a given priority.
// E.g., AllocatableByPriorityAndResourceType[5]["cpu"] is the amount of CPU available to pods with priority 5,
// where alloctable resources = unused resources + resources allocated to lower-priority pods.
type AllocatableByPriorityAndResourceType QuantityByPriorityAndResourceType

func NewAllocatableByPriorityAndResourceType(priorities []int32, resources map[string]resource.Quantity) AllocatableByPriorityAndResourceType {
	rv := make(AllocatableByPriorityAndResourceType)
	for _, priority := range priorities {
		m := make(map[string]resource.Quantity)
		for t, q := range resources {
			m[t] = q.DeepCopy()
		}
		rv[priority] = ResourceList{Resources: m}
	}
	return rv
}

func (m AllocatableByPriorityAndResourceType) DeepCopy() AllocatableByPriorityAndResourceType {
	rv := make(AllocatableByPriorityAndResourceType)
	for priority, resourcesAtPriority := range m {
		rv[priority] = resourcesAtPriority.DeepCopy()
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

// MarkAllocatable indicates resources have been released by pods of priority p,
// thus increasing the resources allocatable to pods of priority p or lower.
func (m AllocatableByPriorityAndResourceType) MarkAllocatable(p int32, rs ResourceList) {
	for priority, allocatableResourcesAtPriority := range m {
		if priority <= p {
			allocatableResourcesAtPriority.Add(rs)
		}
	}
}

// AllocatedByPriorityAndResourceType accounts for resources allocated to pods of a given priority or lower.
// E.g., AllocatedByPriorityAndResourceType[5]["cpu"] is the amount of CPU allocated to pods with priority 5 or lower.
type AllocatedByPriorityAndResourceType QuantityByPriorityAndResourceType

func NewAllocatedByPriorityAndResourceType(priorities []int32) AllocatedByPriorityAndResourceType {
	rv := make(AllocatedByPriorityAndResourceType)
	for _, priority := range priorities {
		rv[priority] = ResourceList{Resources: make(map[string]resource.Quantity)}
	}
	return rv
}

// MarkAllocated increases the resources allocated to pods of priority p or lower.
func (m AllocatedByPriorityAndResourceType) MarkAllocated(p int32, rs ResourceList) {
	for priority, allocatedResourcesAtPriority := range m {
		if priority <= p {
			allocatedResourcesAtPriority.Add(rs)
		}
	}
}

// MarkAllocatable reduces the resources allocated to pods of priority p or lower.
func (m AllocatedByPriorityAndResourceType) MarkAllocatable(p int32, rs ResourceList) {
	for priority, allocatedResourcesAtPriority := range m {
		if priority <= p {
			allocatedResourcesAtPriority.Sub(rs)
		}
	}
}

func (AllocatableByPriorityAndResourceType AllocatableByPriorityAndResourceType) Get(priority int32, resourceType string) resource.Quantity {
	if AllocatableByPriorityAndResourceType == nil {
		return resource.Quantity{}
	}
	quantityByResourceType := AllocatableByPriorityAndResourceType[priority]
	return quantityByResourceType.Get(resourceType)
}

func (assignedByPriorityAndResourceType AllocatedByPriorityAndResourceType) Get(priority int32, resourceType string) resource.Quantity {
	if assignedByPriorityAndResourceType == nil {
		return resource.Quantity{}
	}
	quantityByResourceType := assignedByPriorityAndResourceType[priority]
	return quantityByResourceType.Get(resourceType)
}
