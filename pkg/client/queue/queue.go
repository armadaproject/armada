package queue

import (
	"fmt"

	"github.com/armadaproject/armada/pkg/api"
)

type Queue struct {
	Name           string         `json:"name"`
	Permissions    []Permissions  `json:"permissions"`
	PriorityFactor PriorityFactor `json:"priorityFactor"`
	ResourceLimits ResourceLimits `json:"resourceLimits"`
}

// NewQueue returnes new Queue using the in parameter. Error is returned if
// any of the queue fields has corresponding value in in that is invalid.
func NewQueue(in *api.Queue) (Queue, error) {
	if in == nil {
		return Queue{}, fmt.Errorf("queue is nil")
	}

	priorityFactor, err := NewPriorityFactor(in.PriorityFactor)
	if err != nil {
		return Queue{}, fmt.Errorf("failed to map priority factor. %s", err)
	}

	resourceLimits, err := NewResourceLimits(in.ResourceLimits)
	if err != nil {
		return Queue{}, fmt.Errorf("failed to map resource limits: %v. %s", in.ResourceLimits, err)
	}

	permissions := []Permissions{}
	if len(in.GroupOwners) != 0 || len(in.UserOwners) != 0 {
		permissions = append(permissions, NewPermissionsFromOwners(in.UserOwners, in.GroupOwners))
	}

	for index, permission := range in.Permissions {
		perm, err := NewPermissions(permission)
		if err != nil {
			return Queue{}, fmt.Errorf("failed to map permission with index: %d. %s", index, err)
		}
		permissions = append(permissions, perm)
	}

	return Queue{
		Name: in.Name,
		// Kind:           "Queue",
		PriorityFactor: priorityFactor,
		ResourceLimits: resourceLimits,
		Permissions:    permissions,
	}, nil
}

// ToAPI transforms Queue to *api.Queue structure
func (q Queue) ToAPI() *api.Queue {
	result := &api.Queue{
		Name: q.Name,
		// Kind:           q.Kind,
		PriorityFactor: float64(q.PriorityFactor),
		ResourceLimits: map[string]float64{},
	}

	for resourceName, resourceLimit := range q.ResourceLimits {
		result.ResourceLimits[string(resourceName)] = float64(resourceLimit)
	}

	for _, permission := range q.Permissions {
		result.Permissions = append(result.Permissions, permission.ToAPI())
	}

	return result
}

// HasPermission returns true if the inputSubject is allowed to peform a queue operation
// specified by inputVerb parameter, otherwise returns false
func (q Queue) HasPermission(inputSubject PermissionSubject, inputVerb PermissionVerb) bool {
	for _, permission := range q.Permissions {
		for _, subject := range permission.Subjects {
			if subject == inputSubject {
				for _, verb := range permission.Verbs {
					if verb == inputVerb {
						return true
					}
				}
			}
		}
	}

	return false
}

func QueuesToAPI(queues []Queue) []*api.Queue {
	result := make([]*api.Queue, len(queues))

	for index, queue := range queues {
		result[index] = queue.ToAPI()
	}

	return result
}
