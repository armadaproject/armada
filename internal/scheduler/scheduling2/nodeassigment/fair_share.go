package nodeassigment

import (
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
)

type FairShareNodeAssigner struct {
	nodeDb      model.NodeDb
	evictedJobs []*model.EvictedJob
}

func (a *FairShareNodeAssigner) AssignNode(jctx *context.JobSchedulingContext) (*internaltypes.Node, error) {
	type consideredNode struct {
		node              *internaltypes.Node
		availableResource internaltypes.ResourceList
		evictedJobs       []*model.EvictedJob
	}

	nodesById := make(map[string]*consideredNode)

	for _, evictedJob := range a.evictedJobs {

		// Resolve the node for the evicted job
		node, ok := nodesById[evictedJob.NodeId]
		if !ok {
			dbNode := a.nodeDb.GetNodeById(evictedJob.NodeId)
			node = &consideredNode{
				node:              dbNode,
				availableResource: internaltypes.ResourceList{},
				evictedJobs:       []*model.EvictedJob{},
			}
			nodesById[evictedJob.NodeId] = node
		}

		// Remove the evicted job from the node
		node.availableResource = node.availableResource.Add(evictedJob.Resources)
		node.evictedJobs = append(node.evictedJobs, evictedJob)

		// See if the new job now fits
		dynamicRequirementsMet, _ := nodedb.DynamicJobRequirementsMet(node.availableResource, jctx)
		staticRequirementsMet, _, err := nodedb.StaticJobRequirementsMet(node.node, jctx)
		if err != nil {
			return nil, err
		}

		// If the job now fits, preempt all the evicted jobs on that node
		if dynamicRequirementsMet && staticRequirementsMet {
			for _, preemptedJob := range node.evictedJobs {
				node.node = a.preepmtJob(preemptedJob, node.node)
			}
		}
	}

	return nil
}
