package schedulerobjects

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func ResourceListFromV1ResourceList(rl v1.ResourceList) ResourceList {
	rv := ResourceList{
		Resources: make(map[string]resource.Quantity),
	}
	for t, q := range rl {
		rv.Resources[string(t)] = q
	}
	return rv
}

func V1ResourceListFromResourceList(rl ResourceList) v1.ResourceList {
	rv := make(v1.ResourceList)
	for t, q := range rl.Resources {
		rv[v1.ResourceName(t)] = q.DeepCopy()
	}
	return rv
}

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
	for p, rlb := range b {
		a.AddResourceList(p, rlb)
	}
}

func (a QuantityByPriorityAndResourceType) Sub(b QuantityByPriorityAndResourceType) {
	for p, rlb := range b {
		a.SubResourceList(p, rlb)
	}
}

func (a QuantityByPriorityAndResourceType) AddResourceList(priority int32, rlb ResourceList) {
	rla := a[priority]
	rla.Add(rlb)
	a[priority] = rla
}

func (a QuantityByPriorityAndResourceType) SubResourceList(priority int32, rlb ResourceList) {
	rla := a[priority]
	rla.Sub(rlb)
	a[priority] = rla
}

func (a QuantityByPriorityAndResourceType) Equal(b QuantityByPriorityAndResourceType) bool {
	for p, rla := range a {
		if !rla.Equal(b[p]) {
			return false
		}
	}
	for p, rlb := range b {
		if !rlb.Equal(a[p]) {
			return false
		}
	}
	return true
}

// IsZero returns true if all quantities in a are zero.
func (a QuantityByPriorityAndResourceType) IsZero() bool {
	for _, rl := range a {
		if !rl.IsZero() {
			return false
		}
	}
	return true
}

// IsStrictlyNonNegative returns true if there are no quantities in a with value less than zero.
func (a QuantityByPriorityAndResourceType) IsStrictlyNonNegative() bool {
	for _, rl := range a {
		if !rl.IsStrictlyNonNegative() {
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

// MaxAggregatedByResource updates a in-place such that for each resource type t
// a[p1][t] + ... + a[pn][t] = max(a[p1][t] + ... + a[pn][t], rl[t]),
// where p1, ..., pn are the priorities in a, for each resource set explicitly in rl.
//
// If necessary to add resources to make up the difference, those resources are added at priority p.
func (a QuantityByPriorityAndResourceType) MaxAggregatedByResource(p int32, rl ResourceList) {
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
		a.AddResourceList(p, difference)
	}
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

func (rl *ResourceList) SubQuantity(resourceType string, quantity resource.Quantity) {
	rl.initialise()
	q := rl.Resources[resourceType]
	q.Sub(quantity)
	rl.Resources[resourceType] = q
}

func (rl ResourceList) DeepCopy() ResourceList {
	if rl.Resources == nil {
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

func (a ResourceList) IsZero() bool {
	for _, q := range a.Resources {
		if !q.IsZero() {
			return false
		}
	}
	return true
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

// IsStrictlyNonNegative returns true if there is no quantity in a less than zero.
func (a ResourceList) IsStrictlyNonNegative() bool {
	for _, q := range a.Resources {
		if q.Cmp(resource.Quantity{}) == -1 {
			return false
		}
	}
	return true
}

// IsStrictlyLessOrEqual returns true if all quantities in a are strictly less or equal than those in b.
func (a ResourceList) IsStrictlyLessOrEqual(b ResourceList) bool {
	for t, q := range b.Resources {
		if q.Cmp(a.Get(t)) == -1 {
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

func (rl *ResourceList) initialise() {
	if rl.Resources == nil {
		rl.Resources = make(map[string]resource.Quantity)
	}
}

// AllocatableByPriorityAndResourceType accounts for resources that can be allocated to pods of a given priority.
// E.g., AllocatableByPriorityAndResourceType[5]["cpu"] is the amount of CPU available to pods with priority 5,
// where alloctable resources = unused resources + resources allocated to lower-priority pods.
type AllocatableByPriorityAndResourceType QuantityByPriorityAndResourceType

func NewAllocatableByPriorityAndResourceType(priorities []int32, rl ResourceList) AllocatableByPriorityAndResourceType {
	rv := make(AllocatableByPriorityAndResourceType)
	for _, priority := range priorities {
		rv[priority] = rl.DeepCopy()
	}
	return rv
}

func (m AllocatableByPriorityAndResourceType) DeepCopy() AllocatableByPriorityAndResourceType {
	rv := make(AllocatableByPriorityAndResourceType, len(m))
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
