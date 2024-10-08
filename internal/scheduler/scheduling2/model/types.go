package model

import (
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	v1 "k8s.io/api/core/v1"
)

type JobIterator interface {
	Next() *context.JobSchedulingContext
}

type NodeIterator interface {
	Next() *internaltypes.Node
}

type NodeAssigner interface {
	AssignNodesForGang(gang *context.GangSchedulingContext, evictedJobs []*context.JobSchedulingContext) (GangAssignmentResult, error)
}

type NodeDb interface {
	Txn() NodeDbTransaction
}

type NodeDbTransaction interface {
	// GetNodeById Returns the node with the supplied id or nil if the node doesn't exist
	GetNodeById(id string) *internaltypes.Node

	// AssignJobToNode assigns a job to a node a given priority
	AssignJobToNode(job *jobdb.Job, nodeId string, priority int32)

	// UnassignJobFromNode removes a job from a node
	UnassignJobFromNode(job *jobdb.Job, nodeId string)

	// GetNode returns a node that matches the given predicates
	GetNode(
		priorityClass int32,
		resources internaltypes.ResourceList,
		tolerations []v1.Toleration,
		nodeSelector map[string]string,
		affinity *v1.Affinity) string

	GetAllNodes() NodeIterator

	Commit()

	RollBack()
}

type EvictedJob struct {
	NodeId string
	Job    *jobdb.Job
}

type GangAssignmentResult struct {
	Scheduled            bool
	JobAssignmentResults []AssigmentResult
}

type AssigmentResult struct {
	Scheduled bool
	NodeId    string
	Priority  int32
}

type JobQueue interface {
	Next() *context.GangSchedulingContext
	UpdateQueueCost(queue string, costDelta internaltypes.ResourceList)
	SetOnlyYieldEvicted()
	SetOnlyYieldEvictedForQueue(string)
}
