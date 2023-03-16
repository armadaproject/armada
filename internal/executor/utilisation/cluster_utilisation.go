package utilisation

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/node"
	"github.com/armadaproject/armada/internal/executor/util"
	. "github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/api"
)

type UtilisationService interface {
	GetAvailableClusterCapacity(useLegacyIds bool) (*ClusterAvailableCapacityReport, error)
	GetAllNodeGroupAllocationInfo() ([]*NodeGroupAllocationInfo, error)
}

type ClusterUtilisationService struct {
	clusterContext                context.ClusterContext
	queueUtilisationService       PodUtilisationService
	nodeInfoService               node.NodeInfoService
	usageClient                   api.UsageClient
	trackedNodeLabels             []string
	nodeIdLabel                   string
	nodeReservedResources         armadaresource.ComputeResources
	nodeReservedResourcesPriority int32
}

func NewClusterUtilisationService(
	clusterContext context.ClusterContext,
	queueUtilisationService PodUtilisationService,
	nodeInfoService node.NodeInfoService,
	usageClient api.UsageClient,
	trackedNodeLabels []string,
	nodeIdLabel string,
	nodeReservedResources armadaresource.ComputeResources,
	nodeReservedResourcesPriority int32,
) *ClusterUtilisationService {
	return &ClusterUtilisationService{
		clusterContext:                clusterContext,
		queueUtilisationService:       queueUtilisationService,
		nodeInfoService:               nodeInfoService,
		usageClient:                   usageClient,
		trackedNodeLabels:             trackedNodeLabels,
		nodeIdLabel:                   nodeIdLabel,
		nodeReservedResources:         nodeReservedResources,
		nodeReservedResourcesPriority: nodeReservedResourcesPriority,
	}
}

type NodeGroupAllocationInfo struct {
	NodeType                     *api.NodeTypeIdentifier
	Nodes                        []*v1.Node
	NodeGroupCapacity            armadaresource.ComputeResources
	NodeGroupAllocatableCapacity armadaresource.ComputeResources
	NodeGroupCordonedCapacity    armadaresource.ComputeResources
}

func (clusterUtilisationService *ClusterUtilisationService) ReportClusterUtilisation() {
	allBatchPods, err := clusterUtilisationService.clusterContext.GetActiveBatchPods()
	if err != nil {
		log.Errorf("Failed to get required information to report cluster usage because %s", err)
		return
	}

	nodeGroupInfos, err := clusterUtilisationService.GetAllNodeGroupAllocationInfo()
	if err != nil {
		log.Errorf("Failed to get required information to report cluster usage because %s", err)
		return
	}

	nodeGroupReports := make([]api.NodeTypeUsageReport, 0, len(nodeGroupInfos))
	for _, nodeGroup := range nodeGroupInfos {
		managedPodsOnNodes := GetPodsOnNodes(allBatchPods, nodeGroup.Nodes)
		queueReports := clusterUtilisationService.createReportsOfQueueUsages(managedPodsOnNodes)

		unschedulableNodes := util.
			FilterNodes(nodeGroup.Nodes, func(node *v1.Node) bool { return node.Spec.Unschedulable })

		nodeGroupReports = append(nodeGroupReports, api.NodeTypeUsageReport{
			NodeType:          nodeGroup.NodeType,
			Capacity:          nodeGroup.NodeGroupCapacity,
			AvailableCapacity: nodeGroup.NodeGroupAllocatableCapacity,
			Queues:            queueReports,
			CordonedUsage:     nodeGroup.NodeGroupCordonedCapacity,
			TotalNodes:        int32(len(nodeGroup.Nodes)),
			SchedulableNodes:  int32(len(nodeGroup.Nodes) - len(unschedulableNodes)),
		})
	}

	clusterUsage := api.ClusterUsageReport{
		ClusterId:            clusterUtilisationService.clusterContext.GetClusterId(),
		Pool:                 clusterUtilisationService.clusterContext.GetClusterPool(),
		ReportTime:           time.Now(),
		NodeTypeUsageReports: nodeGroupReports,
	}

	err = clusterUtilisationService.reportUsage(&clusterUsage)

	if err != nil {
		log.Errorf("Failed to report cluster usage because %s", err)
		return
	}
}

type ClusterAvailableCapacityReport struct {
	AvailableCapacity *armadaresource.ComputeResources
	Nodes             []api.NodeInfo
}

func (cls *ClusterUtilisationService) GetAvailableClusterCapacity(useLegacyIds bool) (*ClusterAvailableCapacityReport, error) {
	processingNodes, err := cls.nodeInfoService.GetAllAvailableProcessingNodes()
	if err != nil {
		return nil, errors.Errorf("Failed getting available cluster capacity due to: %s", err)
	}

	allPods, err := cls.clusterContext.GetAllPods()
	if err != nil {
		return nil, errors.Errorf("Failed getting available cluster capacity due to: %s", err)
	}

	allPodsRequiringResource := getAllPodsRequiringResourceOnProcessingNodes(allPods, processingNodes)
	allNonCompletePodsRequiringResource := FilterNonCompletedPods(allPodsRequiringResource)

	totalNodeResource := armadaresource.CalculateTotalResource(processingNodes)
	totalPodResource := armadaresource.CalculateTotalResourceRequest(allNonCompletePodsRequiringResource)

	availableResource := totalNodeResource.DeepCopy()
	availableResource.Sub(totalPodResource)

	nodesUsage := getAllocatedResourceByNodeName(allNonCompletePodsRequiringResource)
	runningPodsByNode := groupPodsByNodes(allNonCompletePodsRequiringResource)
	nodes := make([]api.NodeInfo, 0, len(processingNodes))
	runIdsByNode := cls.getRunIdsByNode(processingNodes, allPods, useLegacyIds)
	for _, n := range processingNodes {
		allocatable := armadaresource.FromResourceList(n.Status.Allocatable)
		available := allocatable.DeepCopy()
		available.Sub(nodesUsage[n.Name])

		runningNodePods := runningPodsByNode[n.Name]
		runningNodePodsNonArmada := util.FilterPods(runningNodePods, func(pod *v1.Pod) bool {
			return !util.IsManagedPod(pod)
		})
		allocated := getAllocatedResourcesByPriority(runningNodePods)
		allocatedNonArmada := getAllocatedResourcesByPriority(runningNodePodsNonArmada)

		reserved := calculateReservedNodeResource(cls.nodeReservedResources, armadaresource.CalculateTotalResourceRequest(runningNodePodsNonArmada))
		addReservedResource(reserved, cls.nodeReservedResourcesPriority, allocatedNonArmada)

		nodes = append(nodes, api.NodeInfo{
			Name:                        n.Name,
			Labels:                      cls.filterTrackedLabels(n.Labels),
			Taints:                      n.Spec.Taints,
			AllocatableResources:        allocatable,
			AvailableResources:          available,
			TotalResources:              allocatable,
			AllocatedResources:          allocated,
			RunIdsByState:               runIdsByNode[n.Name],
			NonArmadaAllocatedResources: allocatedNonArmada,
		})
	}

	return &ClusterAvailableCapacityReport{
		AvailableCapacity: &availableResource, // TODO: This should be the total - max job priority resources.
		Nodes:             nodes,
	}, nil
}

// This returns all the pods assigned the node or soon to be assigned (via node-selector)
// The server api expects job ids, the executor api expects run ids - the legacy flag controls which this returns
func (clusterUtilisationService *ClusterUtilisationService) getRunIdsByNode(nodes []*v1.Node, pods []*v1.Pod, legacy bool) map[string]map[string]api.JobState {
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
		if legacy {
			runId = util.ExtractJobId(pod)
		}

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

func calculateReservedNodeResource(
	reserved armadaresource.ComputeResources,
	existingNodeResource armadaresource.ComputeResources,
) armadaresource.ComputeResources {
	if reserved == nil {
		return armadaresource.ComputeResources{}
	}
	reservedRemaining := reserved
	reservedRemaining.Sub(existingNodeResource)
	reservedRemaining.LimitToZero()
	return reservedRemaining
}

func addReservedResource(
	reserved armadaresource.ComputeResources,
	reservedPriority int32,
	resourceByPriority map[int32]api.ComputeResource,
) {
	if reserved.IsValid() && !reserved.IsZero() {
		if resourceAtPriority, present := resourceByPriority[reservedPriority]; present {
			totalResource := armadaresource.ComputeResources(resourceAtPriority.Resources)
			totalResource.Add(reserved)
			resourceByPriority[reservedPriority] = api.ComputeResource{Resources: totalResource}
		} else {
			resourceByPriority[reservedPriority] = api.ComputeResource{Resources: reserved}
		}
	}
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

func getAllocatedResourcesByPriority(pods []*v1.Pod) map[int32]api.ComputeResource {
	resourceUsageByPriority := make(map[int32]api.ComputeResource)

	podsByPriority := groupPodsByPriority(pods)

	for priority, podsForPriority := range podsByPriority {
		resources := api.ComputeResource{Resources: armadaresource.CalculateTotalResourceRequest(podsForPriority)}
		resourceUsageByPriority[priority] = resources
	}

	return resourceUsageByPriority
}

func groupPodsByPriority(pods []*v1.Pod) map[int32][]*v1.Pod {
	priorityMap := make(map[int32][]*v1.Pod)

	for _, p := range pods {
		var priority int32 = 0
		if p.Spec.Priority != nil {
			priority = *(p.Spec.Priority)
		}
		priorityMap[priority] = append(priorityMap[priority], p)
	}

	return priorityMap
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
	podsOnNodes := GetPodsOnNodes(pods, cordonedNodes)
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
		return map[string]armadaresource.ComputeResources{}, fmt.Errorf("Failed getting total allocatable cluster capacity due to: %s", err)
	}

	allPods, err := clusterUtilisationService.clusterContext.GetAllPods()
	if err != nil {
		return map[string]armadaresource.ComputeResources{}, fmt.Errorf("Failed getting total allocatable cluster capacity due to: %s", err)
	}
	unmanagedPods := FilterPods(allPods, func(pod *v1.Pod) bool {
		return !IsManagedPod(pod)
	})
	activeUnmanagedPods := FilterPodsWithPhase(unmanagedPods, v1.PodRunning)

	nodeGroups := clusterUtilisationService.nodeInfoService.GroupNodesByType(allAvailableProcessingNodes)
	result := map[string]armadaresource.ComputeResources{}

	for _, nodeGroup := range nodeGroups {
		activeUnmanagedPodsOnNodes := GetPodsOnNodes(activeUnmanagedPods, nodeGroup.Nodes)
		unmanagedPodResource := armadaresource.CalculateTotalResourceRequest(activeUnmanagedPodsOnNodes)
		totalNodeGroupResource := armadaresource.CalculateTotalResource(nodeGroup.Nodes)
		allocatableNodeGroupResource := totalNodeGroupResource.DeepCopy()
		allocatableNodeGroupResource.Sub(unmanagedPodResource)
		result[nodeGroup.NodeType.Id] = allocatableNodeGroupResource
	}

	return result, nil
}

func (clusterUtilisationService *ClusterUtilisationService) reportUsage(clusterUsage *api.ClusterUsageReport) error {
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	_, err := clusterUtilisationService.usageClient.ReportUsage(ctx, clusterUsage)

	return err
}

func getAllPodsRequiringResourceOnProcessingNodes(allPods []*v1.Pod, processingNodes []*v1.Node) []*v1.Pod {
	podsUsingResourceOnProcessingNodes := make([]*v1.Pod, 0, len(allPods))

	nodeMap := make(map[string]*v1.Node)
	for _, processingNode := range processingNodes {
		nodeMap[processingNode.Name] = processingNode
	}

	for _, pod := range allPods {
		if _, presentOnProcessingNode := nodeMap[pod.Spec.NodeName]; presentOnProcessingNode {
			podsUsingResourceOnProcessingNodes = append(podsUsingResourceOnProcessingNodes, pod)
		} else if IsManagedPod(pod) && pod.Spec.NodeName == "" {
			podsUsingResourceOnProcessingNodes = append(podsUsingResourceOnProcessingNodes, pod)
		}
	}

	return podsUsingResourceOnProcessingNodes
}

func (clusterUtilisationService *ClusterUtilisationService) createReportsOfQueueUsages(pods []*v1.Pod) []*api.QueueReport {
	podsByQueue := GroupByQueue(pods)
	queueReports := make([]*api.QueueReport, 0, len(podsByQueue))
	for queueName, queuePods := range podsByQueue {
		runningPods := FilterPodsWithPhase(queuePods, v1.PodRunning)
		resourceAllocated := armadaresource.CalculateTotalResourceRequest(runningPods)
		resourceUsed := clusterUtilisationService.getTotalPodUtilisation(queuePods)
		phaseSummary := CountPodsByPhase(queuePods)

		queueReport := api.QueueReport{
			Name:               queueName,
			Resources:          resourceAllocated,
			ResourcesUsed:      resourceUsed,
			CountOfPodsByPhase: phaseSummary,
		}
		queueReports = append(queueReports, &queueReport)
	}
	return queueReports
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
