package utilisation

import (
	"fmt"

	"github.com/armadaproject/armada/pkg/executorapi"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/node"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

type UtilisationService interface {
	GetAvailableClusterCapacity() (*ClusterAvailableCapacityReport, error)
	GetAllNodeGroupAllocationInfo() ([]*NodeGroupAllocationInfo, error)
}

type ClusterUtilisationService struct {
	clusterContext                                                context.ClusterContext
	queueUtilisationService                                       PodUtilisationService
	nodeInfoService                                               node.NodeInfoService
	trackedNodeLabels                                             []string
	nodeIdLabel                                                   string
	minimumResourcesMarkedAllocatedToNonArmadaPodsPerNode         armadaresource.ComputeResources
	minimumResourcesMarkedAllocatedToNonArmadaPodsPerNodePriority int32
}

func NewClusterUtilisationService(
	clusterContext context.ClusterContext,
	queueUtilisationService PodUtilisationService,
	nodeInfoService node.NodeInfoService,
	trackedNodeLabels []string,
	nodeIdLabel string,
	minimumResourcesMarkedAllocatedToNonArmadaPodsPerNode armadaresource.ComputeResources,
	minimumResourcesMarkedAllocatedToNonArmadaPodsPerNodePriority int32,
) *ClusterUtilisationService {
	return &ClusterUtilisationService{
		clusterContext:          clusterContext,
		queueUtilisationService: queueUtilisationService,
		nodeInfoService:         nodeInfoService,
		trackedNodeLabels:       trackedNodeLabels,
		nodeIdLabel:             nodeIdLabel,
		minimumResourcesMarkedAllocatedToNonArmadaPodsPerNode:         minimumResourcesMarkedAllocatedToNonArmadaPodsPerNode,
		minimumResourcesMarkedAllocatedToNonArmadaPodsPerNodePriority: minimumResourcesMarkedAllocatedToNonArmadaPodsPerNodePriority,
	}
}

type NodeGroupAllocationInfo struct {
	NodeType                     *node.NodeTypeIdentifier
	Nodes                        []*v1.Node
	NodeGroupCapacity            armadaresource.ComputeResources
	NodeGroupAllocatableCapacity armadaresource.ComputeResources
	NodeGroupCordonedCapacity    armadaresource.ComputeResources
}

type ClusterAvailableCapacityReport struct {
	AvailableCapacity *armadaresource.ComputeResources
	Nodes             []executorapi.NodeInfo
}

func (cls *ClusterUtilisationService) GetAvailableClusterCapacity() (*ClusterAvailableCapacityReport, error) {
	allNodes, err := cls.nodeInfoService.GetAllNodes()
	if err != nil {
		return nil, errors.Errorf("Failed getting available cluster capacity due to: %s", err)
	}

	allPods, err := cls.clusterContext.GetAllPods()
	if err != nil {
		return nil, errors.Errorf("Failed getting available cluster capacity due to: %s", err)
	}

	allPodsRequiringResource := getAllPodsRequiringResourceOnNodes(allPods, allNodes)
	allNonCompletePodsRequiringResource := util.FilterNonCompletedPods(allPodsRequiringResource)
	nodesUsage := getAllocatedResourceByNodeName(allNonCompletePodsRequiringResource)
	runningPodsByNode := groupPodsByNodes(allNonCompletePodsRequiringResource)
	runIdsByNode := cls.getRunIdsByNode(allNodes, allPods)

	nodes := make([]executorapi.NodeInfo, 0, len(allNodes))
	totalAvailable := armadaresource.ComputeResources{}
	for _, node := range allNodes {
		isSchedulable := cls.nodeInfoService.IsAvailableProcessingNode(node)
		allocatable := armadaresource.FromResourceList(node.Status.Allocatable)
		available := allocatable.DeepCopy()
		available.Sub(nodesUsage[node.Name])

		if isSchedulable {
			totalAvailable.Add(available)
		}

		runningNodePods := runningPodsByNode[node.Name]
		runningNodePodsArmada := util.FilterPods(runningNodePods, func(pod *v1.Pod) bool {
			return util.IsManagedPod(pod)
		})
		runningNodePodsNonArmada := util.FilterPods(runningNodePods, func(pod *v1.Pod) bool {
			return !util.IsManagedPod(pod)
		})
		allocatedByPriority := allocatedByPriorityAndResourceTypeFromPods(runningNodePods)
		allocatedByPriorityNonArmada := allocatedByPriorityAndResourceTypeFromPods(runningNodePodsNonArmada)
		allocatedByPriorityNonArmada.MaxAggregatedByResource(
			cls.minimumResourcesMarkedAllocatedToNonArmadaPodsPerNodePriority,
			schedulerobjects.ResourceList{Resources: cls.minimumResourcesMarkedAllocatedToNonArmadaPodsPerNode},
		)

		usageByQueue := cls.getPodUtilisationByQueue(runningNodePodsArmada)
		resourceUsageByQueue := make(map[string]*executorapi.ComputeResource)
		for queueName, resourceUsage := range usageByQueue {
			resourceUsageByQueue[queueName] = &executorapi.ComputeResource{Resources: resourceUsage}
		}

		nodeAllocatedResources := make(map[int32]executorapi.ComputeResource)
		for p, rl := range allocatedByPriority {
			nodeAllocatedResources[p] = executorapi.ComputeResource{Resources: rl.Resources}
		}
		nodeNonArmadaAllocatedResources := make(map[int32]executorapi.ComputeResource)
		for p, rl := range allocatedByPriorityNonArmada {
			nodeNonArmadaAllocatedResources[p] = executorapi.ComputeResource{Resources: rl.Resources}
		}
		nodes = append(nodes, executorapi.NodeInfo{
			Name:                        node.Name,
			Labels:                      cls.filterTrackedLabels(node.Labels),
			Taints:                      node.Spec.Taints,
			AllocatableResources:        allocatable,
			AvailableResources:          available,
			TotalResources:              allocatable,
			AllocatedResources:          nodeAllocatedResources,
			RunIdsByState:               runIdsByNode[node.Name],
			NonArmadaAllocatedResources: nodeNonArmadaAllocatedResources,
			Unschedulable:               !isSchedulable,
			ResourceUsageByQueue:        resourceUsageByQueue,
			NodeType:                    cls.nodeInfoService.GetType(node).Id,
		})
	}

	return &ClusterAvailableCapacityReport{
		AvailableCapacity: &totalAvailable, // TODO: This should be the total - max job priority resources.
		Nodes:             nodes,
	}, nil
}

// This returns all the pods assigned the node or soon to be assigned (via node-selector)
func (clusterUtilisationService *ClusterUtilisationService) getRunIdsByNode(nodes []*v1.Node, pods []*v1.Pod) map[string]map[string]api.JobState {
	nodeIdToNodeName := make(map[string]string, len(nodes))
	for _, n := range nodes {
		if nodeId, nodeIdPresent := n.Labels[clusterUtilisationService.nodeIdLabel]; nodeIdPresent {
			nodeIdToNodeName[nodeId] = n.Name
		}
	}
	noLongerNeedsReportingFunc := util.IsReportedDone

	result := map[string]map[string]api.JobState{}
	for _, pod := range pods {
		// Skip pods that are not armada pods or "complete" from the servers point of view
		if !util.IsManagedPod(pod) || noLongerNeedsReportingFunc(pod) {
			continue
		}
		nodeIdNodeSelector, nodeSelectorPresent := pod.Spec.NodeSelector[clusterUtilisationService.nodeIdLabel]
		runId := util.ExtractJobRunId(pod)

		nodeName := pod.Spec.NodeName
		if nodeName == "" && nodeSelectorPresent {
			targetedNodeName, present := nodeIdToNodeName[nodeIdNodeSelector]
			// Not scheduled on a node, but has node selector matching the current node
			if present {
				nodeName = targetedNodeName
			}
		}

		if nodeName != "" {
			if _, present := result[nodeName]; !present {
				result[nodeName] = map[string]api.JobState{}
			}
			result[nodeName][runId] = getJobRunState(pod)
		}
	}
	return result
}

func getJobRunState(pod *v1.Pod) api.JobState {
	switch {
	case pod.Status.Phase == v1.PodPending:
		return api.JobState_PENDING
	case pod.Status.Phase == v1.PodRunning:
		return api.JobState_RUNNING
	case pod.Status.Phase == v1.PodSucceeded:
		return api.JobState_SUCCEEDED
	case pod.Status.Phase == v1.PodFailed:
		return api.JobState_FAILED
	}
	return api.JobState_UNKNOWN
}

func getAllocatedResourceByNodeName(pods []*v1.Pod) map[string]armadaresource.ComputeResources {
	allocations := map[string]armadaresource.ComputeResources{}
	for _, pod := range pods {
		nodeName := pod.Spec.NodeName
		resourceRequest := armadaresource.TotalPodResourceRequest(&pod.Spec)

		_, ok := allocations[nodeName]
		if !ok {
			allocations[nodeName] = armadaresource.ComputeResources{}
		}
		allocations[nodeName].Add(resourceRequest)
	}
	return allocations
}

func groupPodsByNodes(pods []*v1.Pod) map[string][]*v1.Pod {
	podsByNodes := make(map[string][]*v1.Pod)

	for _, p := range pods {
		podsByNodes[p.Spec.NodeName] = append(podsByNodes[p.Spec.NodeName], p)
	}

	return podsByNodes
}

func allocatedByPriorityAndResourceTypeFromPods(pods []*v1.Pod) schedulerobjects.QuantityByTAndResourceType[int32] {
	rv := make(schedulerobjects.QuantityByTAndResourceType[int32])
	for _, pod := range pods {
		var priority int32 = 0
		if pod.Spec.Priority != nil {
			priority = *(pod.Spec.Priority)
		}
		request := armadaresource.TotalPodResourceRequest(&pod.Spec)
		rl := schedulerobjects.ResourceList{Resources: request}
		rv.AddResourceList(priority, rl)
	}
	return rv
}

// GetAllNodeGroupAllocationInfo returns allocation information for all nodes on the cluster.
// NodeGroupCapacity is the total capacity of a nodegroup (including cordoned nodes)
// NodeGroupAllocatableCapacity is the capacity available to armada on schedulable nodes
// NodeGroupCordonedCapacity is the resource in use by armada on unschedulable nodes
func (clusterUtilisationService *ClusterUtilisationService) GetAllNodeGroupAllocationInfo() ([]*NodeGroupAllocationInfo, error) {
	allAvailableProcessingNodes, err := clusterUtilisationService.nodeInfoService.GetAllNodes()
	if err != nil {
		return []*NodeGroupAllocationInfo{}, err
	}

	allocatableResourceByNodeType, err := clusterUtilisationService.getAllocatableResourceByNodeType()
	if err != nil {
		return []*NodeGroupAllocationInfo{}, err
	}

	batchPods, err := clusterUtilisationService.clusterContext.GetBatchPods()
	if err != nil {
		return []*NodeGroupAllocationInfo{}, err
	}

	nodeGroups := clusterUtilisationService.nodeInfoService.GroupNodesByType(allAvailableProcessingNodes)
	result := make([]*NodeGroupAllocationInfo, 0, len(nodeGroups))

	for _, nodeGroup := range nodeGroups {
		totalNodeResource := armadaresource.CalculateTotalResource(nodeGroup.Nodes)
		allocatableNodeResource := allocatableResourceByNodeType[nodeGroup.NodeType.Id]
		cordonedNodeResource := getCordonedResource(nodeGroup.Nodes, batchPods)

		result = append(result, &NodeGroupAllocationInfo{
			NodeType:                     nodeGroup.NodeType,
			Nodes:                        nodeGroup.Nodes,
			NodeGroupCapacity:            totalNodeResource,
			NodeGroupAllocatableCapacity: allocatableNodeResource,
			NodeGroupCordonedCapacity:    cordonedNodeResource,
		})
	}

	return result, nil
}

// getCordonedResource takes a list of nodes and a list of pods and returns the resources allocated
// to pods running on cordoned nodes. We need this information in calculating queue fair shares when
// significant resource is running on cordoned nodes.
func getCordonedResource(nodes []*v1.Node, pods []*v1.Pod) armadaresource.ComputeResources {
	cordonedNodes := util.FilterNodes(nodes, func(node *v1.Node) bool { return node.Spec.Unschedulable })
	podsOnNodes := util.GetPodsOnNodes(pods, cordonedNodes)
	usage := armadaresource.ComputeResources{}
	for _, pod := range podsOnNodes {
		for _, container := range pod.Spec.Containers {
			containerResource := armadaresource.FromResourceList(container.Resources.Limits) // Not 100% on whether this should be Requests or Limits
			usage.Add(containerResource)
		}
	}
	return usage
}

// getAllocatableResourceByNodeType returns all allocatable resource currently available on schedulable nodes.
// Resource locked away on cordoned nodes is dealt with in getCordonedResource.
func (clusterUtilisationService *ClusterUtilisationService) getAllocatableResourceByNodeType() (map[string]armadaresource.ComputeResources, error) {
	allAvailableProcessingNodes, err := clusterUtilisationService.nodeInfoService.GetAllAvailableProcessingNodes()
	if err != nil {
		return map[string]armadaresource.ComputeResources{}, fmt.Errorf("failed getting total allocatable cluster capacity due to: %s", err)
	}

	allPods, err := clusterUtilisationService.clusterContext.GetAllPods()
	if err != nil {
		return map[string]armadaresource.ComputeResources{}, fmt.Errorf("failed getting total allocatable cluster capacity due to: %s", err)
	}
	unmanagedPods := util.FilterPods(allPods, func(pod *v1.Pod) bool {
		return !util.IsManagedPod(pod)
	})
	activeUnmanagedPods := util.FilterPodsWithPhase(unmanagedPods, v1.PodRunning)

	nodeGroups := clusterUtilisationService.nodeInfoService.GroupNodesByType(allAvailableProcessingNodes)
	result := map[string]armadaresource.ComputeResources{}

	for _, nodeGroup := range nodeGroups {
		activeUnmanagedPodsOnNodes := util.GetPodsOnNodes(activeUnmanagedPods, nodeGroup.Nodes)
		unmanagedPodResource := armadaresource.CalculateTotalResourceRequest(activeUnmanagedPodsOnNodes)
		totalNodeGroupResource := armadaresource.CalculateTotalResource(nodeGroup.Nodes)
		allocatableNodeGroupResource := totalNodeGroupResource.DeepCopy()
		allocatableNodeGroupResource.Sub(unmanagedPodResource)
		result[nodeGroup.NodeType.Id] = allocatableNodeGroupResource
	}

	return result, nil
}

func getAllPodsRequiringResourceOnNodes(allPods []*v1.Pod, nodes []*v1.Node) []*v1.Pod {
	podsUsingResourceOnProcessingNodes := make([]*v1.Pod, 0, len(allPods))

	nodeMap := make(map[string]*v1.Node)
	for _, processingNode := range nodes {
		nodeMap[processingNode.Name] = processingNode
	}

	for _, pod := range allPods {
		if _, presentOnNode := nodeMap[pod.Spec.NodeName]; presentOnNode {
			podsUsingResourceOnProcessingNodes = append(podsUsingResourceOnProcessingNodes, pod)
		} else if util.IsManagedPod(pod) && pod.Spec.NodeName == "" {
			podsUsingResourceOnProcessingNodes = append(podsUsingResourceOnProcessingNodes, pod)
		}
	}

	return podsUsingResourceOnProcessingNodes
}

func (clusterUtilisationService *ClusterUtilisationService) getPodUtilisationByQueue(pods []*v1.Pod) map[string]armadaresource.ComputeResources {
	podsByQueue := util.GroupByQueue(pods)
	result := make(map[string]armadaresource.ComputeResources, len(podsByQueue))
	for queueName, queuePods := range podsByQueue {
		resourceUsed := clusterUtilisationService.getTotalPodUtilisation(queuePods)
		result[queueName] = resourceUsed
	}
	return result
}

func (clusterUtilisationService *ClusterUtilisationService) getTotalPodUtilisation(pods []*v1.Pod) armadaresource.ComputeResources {
	totalUtilisation := armadaresource.ComputeResources{}

	for _, pod := range pods {
		podUsage := clusterUtilisationService.queueUtilisationService.GetPodUtilisation(pod)
		totalUtilisation.Add(podUsage.CurrentUsage)
	}

	return totalUtilisation
}

func (clusterUtilisationService *ClusterUtilisationService) filterTrackedLabels(labels map[string]string) map[string]string {
	result := map[string]string{}
	for _, k := range clusterUtilisationService.trackedNodeLabels {
		v, ok := labels[k]
		if ok {
			result[k] = v
		}
	}
	return result
}

func GetAllocationByQueue(pods []*v1.Pod) map[string]armadaresource.ComputeResources {
	utilisationByQueue := make(map[string]armadaresource.ComputeResources)

	for _, pod := range pods {
		queue, present := pod.Labels[domain.Queue]
		if !present {
			log.Errorf("Pod %s found not belonging to a queue, not reporting its allocation", pod.Name)
			continue
		}

		podAllocatedResourece := armadaresource.CalculateTotalResourceRequest([]*v1.Pod{pod})
		if _, ok := utilisationByQueue[queue]; ok {
			utilisationByQueue[queue].Add(podAllocatedResourece)
		} else {
			utilisationByQueue[queue] = podAllocatedResourece
		}
	}

	return utilisationByQueue
}

func GetAllocationByQueueAndPriority(pods []*v1.Pod) map[string]map[int32]armadaresource.ComputeResources {
	rv := make(map[string]map[int32]armadaresource.ComputeResources)
	for _, pod := range pods {

		// Get the name of the queue this pod originated from,
		// ignoring pods for which we can't find the queue.
		queue, present := pod.Labels[domain.Queue]
		if !present {
			log.Errorf("Pod %s found not belonging to a queue, not reporting its allocation", pod.Name)
			continue
		}

		// Get the priority of this pod.
		var priority int32
		if pod.Spec.Priority != nil {
			priority = *pod.Spec.Priority
		}

		// Get total pod resource usage and add it to the aggregate.
		podAllocatedResourece := armadaresource.CalculateTotalResourceRequest([]*v1.Pod{pod})
		allocatedByPriority, ok := rv[queue]
		if !ok {
			allocatedByPriority = make(map[int32]armadaresource.ComputeResources)
			rv[queue] = allocatedByPriority
		}
		if allocated, ok := allocatedByPriority[priority]; ok {
			allocated.Add(podAllocatedResourece)
		} else {
			allocatedByPriority[priority] = podAllocatedResourece
		}
	}
	return rv
}
