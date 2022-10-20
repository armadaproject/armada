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

// AvailableByPriorityAndResourceType accounts for resources available to pods of a given priority.
// E.g., AvailableByPriorityAndResourceType[5]["cpu"] is the amount of CPU available to pods with priority 5,
// where available resources = unused resources + resources assigned to lower-priority pods.
type AvailableByPriorityAndResourceType QuantityByPriorityAndResourceType

func NewAvailableByPriorityAndResourceType(priorities []int32, resources map[string]resource.Quantity) AvailableByPriorityAndResourceType {
	rv := make(AvailableByPriorityAndResourceType)
	for _, priority := range priorities {
		m := make(map[string]resource.Quantity)
		for t, q := range resources {
			m[t] = q.DeepCopy()
		}
		rv[priority] = ResourceList{Resources: m}
	}
	return rv
}

func (m AvailableByPriorityAndResourceType) DeepCopy() AvailableByPriorityAndResourceType {
	rv := make(AvailableByPriorityAndResourceType)
	for priority, resourcesAtPriority := range m {
		rv[priority] = ResourceList{Resources: make(map[string]resource.Quantity)}
		for resourceType, quantity := range resourcesAtPriority.Resources {
			m[priority].Resources[resourceType] = quantity.DeepCopy()
		}
	}
	return rv
}

// MarkUsed reduces the resources available to pods of priority p or lower.
func (m AvailableByPriorityAndResourceType) MarkUsed(p int32, rs ResourceList) {
	for priority, availableResourcesAtPriority := range m {
		if priority <= p {
			for usedResourceType, usedResourceQuantity := range rs.Resources {
				q := availableResourcesAtPriority.Resources[usedResourceType]
				if q.Cmp(usedResourceQuantity) == -1 {
					q.Set(0)
				} else {
					q.Sub(usedResourceQuantity)
				}
				availableResourcesAtPriority.Resources[usedResourceType] = q
			}
		}
	}
}

// MarkAvailable increases the resources available to pods of priority p or higher.
func (m AvailableByPriorityAndResourceType) MarkAvailable(p int32, rs ResourceList) {
	for priority, availableResourcesAtPriority := range m {
		if priority <= p {
			for usedResourceType, usedResourceQuantity := range rs.Resources {
				q := availableResourcesAtPriority.Resources[usedResourceType]
				q.Add(usedResourceQuantity)
				availableResourcesAtPriority.Resources[usedResourceType] = q
			}
		}
	}
}

// AssignedByPriorityAndResourceType accounts for resources assigned to pods of a given priority or lower.
// E.g., AssignedByPriorityAndResourceType[5]["cpu"] is the amount of CPU assigned to pods with priority 5 or lower.
type AssignedByPriorityAndResourceType QuantityByPriorityAndResourceType

func NewAssignedByPriorityAndResourceType(priorities []int32) AssignedByPriorityAndResourceType {
	rv := make(AssignedByPriorityAndResourceType)
	for _, priority := range priorities {
		rv[priority] = ResourceList{Resources: make(map[string]resource.Quantity)}
	}
	return rv
}

// MarkUsed increases the resources assigned to pods of priority p or lower.
func (m AssignedByPriorityAndResourceType) MarkUsed(p int32, rs ResourceList) {
	for priority, assignedResourcesAtPriority := range m {
		if priority <= p {
			for usedResourceType, usedResourceQuantity := range rs.Resources {
				q := assignedResourcesAtPriority.Resources[usedResourceType]
				q.Add(usedResourceQuantity)
				assignedResourcesAtPriority.Resources[usedResourceType] = q
			}
		}
	}
}

// MarkAvailable reduces the resources assigned to pods of priority p or lower.
func (m AssignedByPriorityAndResourceType) MarkAvailable(p int32, rs ResourceList) {
	for priority, assignedResourcesAtPriority := range m {
		if priority <= p {
			for usedResourceType, usedResourceQuantity := range rs.Resources {
				q := assignedResourcesAtPriority.Resources[usedResourceType]
				if q.Cmp(usedResourceQuantity) == -1 {
					q.Set(0)
				} else {
					q.Sub(usedResourceQuantity)
				}
				assignedResourcesAtPriority.Resources[usedResourceType] = q
			}
		}
	}
}

func (availableByPriorityAndResourceType AvailableByPriorityAndResourceType) Get(priority int32, resourceType string) resource.Quantity {
	if availableByPriorityAndResourceType == nil {
		return resource.MustParse("0")
	}
	quantityByResourceType, ok := availableByPriorityAndResourceType[priority]
	if !ok {
		return resource.MustParse("0")
	}
	q, ok := quantityByResourceType.Resources[resourceType]
	if !ok {
		return resource.MustParse("0")
	}
	return q
}

func (assignedByPriorityAndResourceType AssignedByPriorityAndResourceType) Get(priority int32, resourceType string) resource.Quantity {
	if assignedByPriorityAndResourceType == nil {
		return resource.MustParse("0")
	}
	quantityByResourceType, ok := assignedByPriorityAndResourceType[priority]
	if !ok {
		return resource.MustParse("0")
	}
	q, ok := quantityByResourceType.Resources[resourceType]
	if !ok {
		return resource.MustParse("0")
	}
	return q
}
