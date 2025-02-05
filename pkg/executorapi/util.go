package executorapi

import (
	protoutil "github.com/armadaproject/armada/internal/common/proto"


	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
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
		ResourceListFromProtoResources(nodeInfo.TotalResources),
	)
	for p, rl := range nodeInfo.NonArmadaAllocatedResources {
		allocatableByPriorityAndResource.MarkAllocated(p, ResourceListFromProtoResources(rl.Resources))
	}
	unallocatableResources := make(map[int32]schedulerobjects.ResourceList)
	for p, rl := range nodeInfo.NonArmadaAllocatedResources {
		unallocatableResources[p] = ResourceListFromProtoResources(rl.Resources)
	}
	resourceUsageByQueueAndPool := make([]*schedulerobjects.PoolQueueResource, len(nodeInfo.ResourceUsageByQueueAndPool))
	for i, resourceUsage := range nodeInfo.ResourceUsageByQueueAndPool {
		rl := ResourceListFromProtoResources(resourceUsage.Resources)
		resourceUsageByQueueAndPool[i] = &schedulerobjects.PoolQueueResource{
			Pool:      resourceUsage.Pool,
			Queue:     resourceUsage.Queue,
			Resources: &rl,
		}
	}

	jobRunsByState := make(map[string]schedulerobjects.JobRunState)
	for jobId, state := range nodeInfo.RunIdsByState {
		jobRunsByState[jobId] = api.JobRunStateFromApiJobState(state)
	}
	return &schedulerobjects.Node{
		Id:       api.NodeIdFromExecutorAndNodeName(executor, nodeInfo.Name),
		Name:     nodeInfo.Name,
		Executor: executor,
		Pool:     nodeInfo.Pool,
		LastSeen: protoutil.ToTimestamp(lastSeen),
		Taints: armadaslices.Map(nodeInfo.GetTaints(), func(v *v1.Taint) v1.Taint {
			if v != nil {
				return *v
			}
			return v1.Taint{}
		}),
		Labels:                           nodeInfo.GetLabels(),
		TotalResources:                   ResourceListFromProtoResources(nodeInfo.TotalResources),
		AllocatableByPriorityAndResource: allocatableByPriorityAndResource,
		UnallocatableResources:           unallocatableResources,
		StateByJobRunId:                  jobRunsByState,
		Unschedulable:                    nodeInfo.Unschedulable,
		ResourceUsageByQueueAndPool:      resourceUsageByQueueAndPool,
		ReportingNodeType:                nodeInfo.NodeType,
	}, nil
}

func ResourceListFromProtoResources(r map[string]*resource.Quantity) schedulerobjects.ResourceList {
	return schedulerobjects.ResourceList{
		Resources: armadamaps.MapValues(r, func(v *resource.Quantity) resource.Quantity {
			if v != nil {
				return *v
			}
			return resource.Quantity{}
		}),
	}
}

func ComputeResourceFromProtoResources(r map[string]resource.Quantity) *ComputeResource {
	resources := make(map[string]*resource.Quantity, len(r))
	for k, v := range r {
		r := v.DeepCopy()
		resources[k] = &r
	}
	return &ComputeResource{Resources: resources}
}
