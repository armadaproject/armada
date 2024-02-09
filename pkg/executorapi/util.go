package executorapi

import (
	"time"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

func NewNodeFromNodeInfo(nodeInfo *NodeInfo, executor string, allowedPriorities []int32, lastSeen time.Time) (*schedulerobjects.Node, error) {
	if executor == "" {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "executor",
			Value:   executor,
			Message: "executor is empty",
		})
	}
	if nodeInfo.Name == "" {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "nodeInfo.Name",
			Value:   nodeInfo.Name,
			Message: "nodeInfo.Name is empty",
		})
	}

	allocatableByPriorityAndResource := schedulerobjects.NewAllocatableByPriorityAndResourceType(
		allowedPriorities,
		schedulerobjects.ResourceList{
			Resources: nodeInfo.TotalResources,
		},
	)
	for p, rl := range nodeInfo.NonArmadaAllocatedResources {
		allocatableByPriorityAndResource.MarkAllocated(p, schedulerobjects.ResourceList{Resources: rl.Resources})
	}
	nonArmadaAllocatedResources := make(map[int32]schedulerobjects.ResourceList)
	for p, rl := range nodeInfo.NonArmadaAllocatedResources {
		nonArmadaAllocatedResources[p] = schedulerobjects.ResourceList{Resources: rl.Resources}
	}
	resourceUsageByQueue := make(map[string]*schedulerobjects.ResourceList)
	for queueName, resourceUsage := range nodeInfo.ResourceUsageByQueue {
		resourceUsageByQueue[queueName] = &schedulerobjects.ResourceList{Resources: resourceUsage.Resources}
	}

	jobRunsByState := make(map[string]schedulerobjects.JobRunState)
	for jobId, state := range nodeInfo.RunIdsByState {
		jobRunsByState[jobId] = api.JobRunStateFromApiJobState(state)
	}
	return &schedulerobjects.Node{
		Id:                               api.NodeIdFromExecutorAndNodeName(executor, nodeInfo.Name),
		Name:                             nodeInfo.Name,
		Executor:                         executor,
		LastSeen:                         lastSeen,
		Taints:                           nodeInfo.GetTaints(),
		Labels:                           nodeInfo.GetLabels(),
		TotalResources:                   schedulerobjects.ResourceList{Resources: nodeInfo.TotalResources},
		AllocatableByPriorityAndResource: allocatableByPriorityAndResource,
		NonArmadaAllocatedResources:      nonArmadaAllocatedResources,
		StateByJobRunId:                  jobRunsByState,
		Unschedulable:                    nodeInfo.Unschedulable,
		ResourceUsageByQueue:             resourceUsageByQueue,
		ReportingNodeType:                nodeInfo.NodeType,
	}, nil
}
