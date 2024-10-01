package model

import (
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type JobIterator interface {
	Next() *context.JobSchedulingContext
}

type NodeIterator interface {
	Next() *internaltypes.Node
}

type NodeAssigner interface {
	AssignNode(job *context.JobSchedulingContext) (*internaltypes.Node, error)
}

type NodeDb interface {
	// GetNodeById Returns the node with the supplied id or nil if the node doesn't exist
	GetNodeById(id string) *internaltypes.Node

	// UpsertNode updates the node in the NodeDb. If the node doesn't exist, it will be created
	UpsertNode(node *internaltypes.Node)

	// GetNodes returns an iterator over all node matching the given predicates
	GetNodes(
		priorityClass int32,
		resources internaltypes.ResourceList,
		tolerations []v1.Toleration,
		nodeSelector map[string]string,
		affinity *v1.Affinity) NodeIterator
}

type EvictedJob struct {
	JobId         string
	NodeId        string
	PriorityClass string
	Resources     internaltypes.ResourceList
}
