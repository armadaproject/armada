package schedulerobjects

import (
	"fmt"
	"math"
	"strings"

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

// type QuantityByPriorityAndResourceType QuantityByTAndResourceType[int32]

func (a QuantityByTAndResourceType[T]) Add(b QuantityByTAndResourceType[T]) {
	for p, rlb := range b {
		a.AddResourceList(p, rlb)
	}
}

func (a QuantityByTAndResourceType[T]) AddResourceList(t T, rlb ResourceList) {
	rla := a[t]
	rla.Add(rlb)
	a[t] = rla
}

func (a QuantityByTAndResourceType[T]) DeepCopy() QuantityByTAndResourceType[T] {
	rv := make(QuantityByTAndResourceType[T])
	for t, rl := range a {
		rv[t] = rl.DeepCopy()
	}
	return rv
}

func (a QuantityByTAndResourceType[T]) String() string {
	var sb strings.Builder
	i := 0
	sb.WriteString("{")
	for t, rl := range a {
		if i < len(a)-1 {
			sb.WriteString(fmt.Sprintf("%v: %s, ", t, rl.CompactString()))
		} else {
			sb.WriteString(fmt.Sprintf("%v: %s", t, rl.CompactString()))
		}
	}
	sb.WriteString("}")
	return sb.String()
}

func (a QuantityByTAndResourceType[T]) Sub(b QuantityByTAndResourceType[T]) {
	for t, rlb := range b {
		a.SubResourceList(t, rlb)
	}
}

func (a QuantityByTAndResourceType[T]) AddV1ResourceList(t T, rlb v1.ResourceList) {
	rla := a[t]
	rla.AddV1ResourceList(rlb)
	a[t] = rla
}

func (a QuantityByTAndResourceType[T]) SubResourceList(t T, rlb ResourceList) {
	rla := a[t]
	rla.Sub(rlb)
	a[t] = rla
}

func (a QuantityByTAndResourceType[T]) SubV1ResourceList(t T, rlb v1.ResourceList) {
	rla := a[t]
	rla.SubV1ResourceList(rlb)
	a[t] = rla
}

func (a QuantityByTAndResourceType[T]) Equal(b QuantityByTAndResourceType[T]) bool {
	for t, rla := range a {
		if !rla.Equal(b[t]) {
			return false
		}
	}
	for t, rlb := range b {
		if !rlb.Equal(a[t]) {
			return false
		}
	}
	return true
}

// IsZero returns true if all quantities in a are zero.
func (a QuantityByTAndResourceType[T]) IsZero() bool {
	for _, rl := range a {
		if !rl.IsZero() {
			return false
		}
	}
	return true
}

// IsStrictlyNonNegative returns true if there are no quantities in a with value less than zero.
func (a QuantityByTAndResourceType[T]) IsStrictlyNonNegative() bool {
	for _, rl := range a {
		if !rl.IsStrictlyNonNegative() {
			return false
		}
	}
	return true
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

func (rl *ResourceList) SubQuantity(resourceType string, quantity resource.Quantity) {
	rl.initialise()
	q := rl.Resources[resourceType]
	q.Sub(quantity)
	rl.Resources[resourceType] = q
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

// IsStrictlyNonNegative returns true if there is no quantity less than zero.
func (a ResourceList) IsStrictlyNonNegative() bool {
	for _, q := range a.Resources {
		if q.Cmp(resource.Quantity{}) == -1 {
			return false
		}
	}
	return true
}

// IsStrictlyLessOrEqual returns false if there is a quantity in b greater than that in a and true otherwise.
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

// AsWeightedMillis returns the linear combination of the milli values in rl with given weights.
// This function overflows for values greater than MaxInt64. E.g., 1Pi is fine but not 10Pi.
func (rl *ResourceList) AsWeightedMillis(weights map[string]float64) int64 {
	var rv int64
	for t, w := range weights {
		q := rl.Get(t)
		rv += int64(math.Round(float64(q.MilliValue()) * w))
	}
	return rv
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

func (m AllocatableByPriorityAndResourceType) MarkAllocatedV1ResourceList(p int32, rs v1.ResourceList) {
	for priority, allocatableResourcesAtPriority := range m {
		if priority <= p {
			allocatableResourcesAtPriority.SubV1ResourceList(rs)
		}
	}
}

func (m AllocatableByPriorityAndResourceType) MarkAllocatableV1ResourceList(p int32, rs v1.ResourceList) {
	for priority, allocatableResourcesAtPriority := range m {
		if priority <= p {
			allocatableResourcesAtPriority.AddV1ResourceList(rs)
		}
	}
}

// AllocatedByPriorityAndResourceType accounts for resources allocated to pods of a given priority or lower.
// E.g., AllocatedByPriorityAndResourceType[5]["cpu"] is the amount of CPU allocated to pods with priority 5 or lower.
type AllocatedByPriorityAndResourceType QuantityByTAndResourceType[int32]

func NewAllocatedByPriorityAndResourceType(priorities []int32) AllocatedByPriorityAndResourceType {
	rv := make(AllocatedByPriorityAndResourceType)
	for _, priority := range priorities {
		rv[priority] = NewResourceListWithDefaultSize()
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

func (allocatableByPriorityAndResourceType AllocatableByPriorityAndResourceType) Get(priority int32, resourceType string) resource.Quantity {
	if allocatableByPriorityAndResourceType == nil {
		return resource.Quantity{}
	}
	quantityByResourceType := allocatableByPriorityAndResourceType[priority]
	return quantityByResourceType.Get(resourceType)
}

func (assignedByPriorityAndResourceType AllocatedByPriorityAndResourceType) Get(priority int32, resourceType string) resource.Quantity {
	if assignedByPriorityAndResourceType == nil {
		return resource.Quantity{}
	}
	quantityByResourceType := assignedByPriorityAndResourceType[priority]
	return quantityByResourceType.Get(resourceType)
}
