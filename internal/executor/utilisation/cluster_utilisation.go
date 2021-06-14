package utilisation

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/domain"
	. "github.com/G-Research/armada/internal/executor/util"
	"github.com/G-Research/armada/pkg/api"
)

type UtilisationService interface {
	GetAvailableClusterCapacity() (*ClusterAvailableCapacityReport, error)
	GetTotalAllocatableClusterCapacity() (*common.ComputeResources, error)
	GetAllAvailableProcessingNodes() ([]*v1.Node, error)
}

type ClusterUtilisationService struct {
	clusterContext          context.ClusterContext
	queueUtilisationService PodUtilisationService
	usageClient             api.UsageClient
	trackedNodeLabels       []string
	toleratedTaints         map[string]bool
}

func NewClusterUtilisationService(
	clusterContext context.ClusterContext,
	queueUtilisationService PodUtilisationService,
	usageClient api.UsageClient,
	trackedNodeLabels []string,
	toleratedTaints []string) *ClusterUtilisationService {

	return &ClusterUtilisationService{
		clusterContext:          clusterContext,
		queueUtilisationService: queueUtilisationService,
		usageClient:             usageClient,
		trackedNodeLabels:       trackedNodeLabels,
		toleratedTaints:         util.StringListToSet(toleratedTaints),
	}
}

func (clusterUtilisationService *ClusterUtilisationService) ReportClusterUtilisation() {
	allAvailableProcessingNodes, err := clusterUtilisationService.GetAllAvailableProcessingNodes()
	if err != nil {
		log.Errorf("Failed to get required information to report cluster usage because %s", err)
		return
	}

	totalNodeResource := common.CalculateTotalResource(allAvailableProcessingNodes)

	allActiveManagedPods, err := clusterUtilisationService.getAllRunningManagedPods()
	if err != nil {
		log.Errorf("Failed to get required information to report cluster usage because %s", err)
		return
	}

	allocatableClusterCapacity, err := clusterUtilisationService.GetTotalAllocatableClusterCapacity()
	if err != nil {
		log.Errorf("Failed to get required information to report cluster usage because %s", err)
		return
	}

	queueReports := clusterUtilisationService.createReportsOfQueueUsages(allActiveManagedPods)

	clusterUsage := api.ClusterUsageReport{
		ClusterId:                clusterUtilisationService.clusterContext.GetClusterId(),
		Pool:                     clusterUtilisationService.clusterContext.GetClusterPool(),
		ReportTime:               time.Now(),
		Queues:                   queueReports,
		ClusterCapacity:          totalNodeResource,
		ClusterAvailableCapacity: *allocatableClusterCapacity,
	}

	err = clusterUtilisationService.reportUsage(&clusterUsage)

	if err != nil {
		log.Errorf("Failed to report cluster usage because %s", err)
		return
	}
}

type ClusterAvailableCapacityReport struct {
	AvailableCapacity *common.ComputeResources
	Nodes             []api.NodeInfo
}

func (clusterUtilisationService *ClusterUtilisationService) GetAvailableClusterCapacity() (*ClusterAvailableCapacityReport, error) {
	processingNodes, err := clusterUtilisationService.GetAllAvailableProcessingNodes()
	if err != nil {
		return nil, fmt.Errorf("Failed getting available cluster capacity due to: %s", err)
	}

	allPods, err := clusterUtilisationService.clusterContext.GetAllPods()
	if err != nil {
		return nil, fmt.Errorf("Failed getting available cluster capacity due to: %s", err)
	}

	allPodsRequiringResource := getAllPodsRequiringResourceOnProcessingNodes(allPods, processingNodes)
	allNonCompletePodsRequiringResource := FilterNonCompletedPods(allPodsRequiringResource)

	totalNodeResource := common.CalculateTotalResource(processingNodes)
	totalPodResource := common.CalculateTotalResourceRequest(allNonCompletePodsRequiringResource)

	availableResource := totalNodeResource.DeepCopy()
	availableResource.Sub(totalPodResource)

	nodesUsage := getAllocatedResourceByNodeName(allNonCompletePodsRequiringResource)
	nodes := []api.NodeInfo{}
	for _, n := range processingNodes {
		allocatable := common.FromResourceList(n.Status.Allocatable)
		available := allocatable.DeepCopy()
		available.Sub(nodesUsage[n.Name])

		nodes = append(nodes, api.NodeInfo{
			Name:                 n.Name,
			Labels:               clusterUtilisationService.filterTrackedLabels(n.Labels),
			Taints:               n.Spec.Taints,
			AllocatableResources: allocatable,
			AvailableResources:   available,
		})
	}

	return &ClusterAvailableCapacityReport{
		AvailableCapacity: &availableResource,
		Nodes:             nodes,
	}, nil
}

func getAllocatedResourceByNodeName(pods []*v1.Pod) map[string]common.ComputeResources {
	allocations := map[string]common.ComputeResources{}
	for _, pod := range pods {
		nodeName := pod.Spec.NodeName
		resourceRequest := common.TotalPodResourceRequest(&pod.Spec)

		_, ok := allocations[nodeName]
		if !ok {
			allocations[nodeName] = common.ComputeResources{}
		}
		allocations[nodeName].Add(resourceRequest)
	}
	return allocations
}

func (clusterUtilisationService *ClusterUtilisationService) GetTotalAllocatableClusterCapacity() (*common.ComputeResources, error) {
	allAvailableProcessingNodes, err := clusterUtilisationService.GetAllAvailableProcessingNodes()
	if err != nil {
		return new(common.ComputeResources), fmt.Errorf("Failed getting total allocatable cluster capacity due to: %s", err)
	}

	allPods, err := clusterUtilisationService.clusterContext.GetAllPods()
	if err != nil {
		return new(common.ComputeResources), fmt.Errorf("Failed getting total allocatable cluster capacity due to: %s", err)
	}

	totalNodeResource := common.CalculateTotalResource(allAvailableProcessingNodes)
	resourceOfUnmanagedPodsOnProcessingNodes := getResourceRequiredByUnmanagedPodsOnNodes(allPods, allAvailableProcessingNodes)
	allocatableClusterCapacity := totalNodeResource.DeepCopy()
	allocatableClusterCapacity.Sub(resourceOfUnmanagedPodsOnProcessingNodes)

	return &allocatableClusterCapacity, nil
}

func getResourceRequiredByUnmanagedPodsOnNodes(allPods []*v1.Pod, nodes []*v1.Node) common.ComputeResources {
	unmanagedPodsOnNodes := getUnmanagedPodsByNode(allPods, nodes)

	totalResource := common.ComputeResources{}

	for _, pods := range unmanagedPodsOnNodes {
		totalResource.Add(common.CalculateTotalResourceRequest(pods))
	}
	return totalResource
}

func getUnmanagedPodsByNode(allPods []*v1.Pod, nodes []*v1.Node) map[*v1.Node][]*v1.Pod {
	unmanagedPods := FilterPods(allPods, func(pod *v1.Pod) bool {
		return !IsManagedPod(pod)
	})
	activeUnmanagedPods := FilterPodsWithPhase(unmanagedPods, v1.PodRunning)

	return getPodsByNode(activeUnmanagedPods, nodes)
}

func (clusterUtilisationService *ClusterUtilisationService) GetAllAvailableProcessingNodes() ([]*v1.Node, error) {
	allNodes, err := clusterUtilisationService.clusterContext.GetNodes()
	if err != nil {
		return []*v1.Node{}, err
	}

	return clusterUtilisationService.filterAvailableProcessingNodes(allNodes), nil
}

func (clusterUtilisationService *ClusterUtilisationService) reportUsage(clusterUsage *api.ClusterUsageReport) error {
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	_, err := clusterUtilisationService.usageClient.ReportUsage(ctx, clusterUsage)

	return err
}

func (clusterUtilisationService *ClusterUtilisationService) filterAvailableProcessingNodes(nodes []*v1.Node) []*v1.Node {
	processingNodes := make([]*v1.Node, 0, len(nodes))

	for _, node := range nodes {
		if clusterUtilisationService.isAvailableProcessingNode(node) {
			processingNodes = append(processingNodes, node)
		}
	}

	return processingNodes
}

func (clusterUtilisationService *ClusterUtilisationService) isAvailableProcessingNode(node *v1.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}

	for _, taint := range node.Spec.Taints {
		if taint.Effect == v1.TaintEffectNoSchedule &&
			!clusterUtilisationService.toleratedTaints[taint.Key] {
			return false
		}
	}

	return true
}

func getAllPodsRequiringResourceOnProcessingNodes(allPods []*v1.Pod, processingNodes []*v1.Node) []*v1.Pod {
	podsUsingResourceOnProcessingNodes := make([]*v1.Pod, 0, len(allPods))

	nodeMap := make(map[string]*v1.Node)
	for _, node := range processingNodes {
		nodeMap[node.Name] = node
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

func getPodsByNode(pods []*v1.Pod, nodes []*v1.Node) map[*v1.Node][]*v1.Pod {
	nodeSet := make(map[string]*v1.Node)
	for _, node := range nodes {
		nodeSet[node.Name] = node
	}

	podsOnNodes := make(map[*v1.Node][]*v1.Pod, len(nodes))
	for _, node := range nodes {
		podsOnNodes[node] = make([]*v1.Pod, 0, 10)
	}

	for _, pod := range pods {
		if node, presentOnProcessingNode := nodeSet[pod.Spec.NodeName]; presentOnProcessingNode {
			podsOnNodes[node] = append(podsOnNodes[node], pod)
		}
	}

	return podsOnNodes
}

func (clusterUtilisationService *ClusterUtilisationService) getAllRunningManagedPods() ([]*v1.Pod, error) {
	allActiveManagedPods, err := clusterUtilisationService.clusterContext.GetActiveBatchPods()
	if err != nil {
		return []*v1.Pod{}, err
	}
	allActiveManagedPods = FilterPodsWithPhase(allActiveManagedPods, v1.PodRunning)
	return allActiveManagedPods, nil
}

func (clusterUtilisationService *ClusterUtilisationService) createReportsOfQueueUsages(pods []*v1.Pod) []*api.QueueReport {
	allocationByQueue := GetAllocationByQueue(pods)
	usageByQueue := clusterUtilisationService.getUsageByQueue(pods)

	queueReports := make([]*api.QueueReport, 0, len(allocationByQueue))

	for queueName, queueUsage := range allocationByQueue {
		queueUtilisation, present := usageByQueue[queueName]
		var resourceUsed common.ComputeResources
		if !present {
			resourceUsed = *new(common.ComputeResources)
		} else {
			resourceUsed = queueUtilisation
		}
		queueReport := api.QueueReport{
			Name:          queueName,
			Resources:     queueUsage,
			ResourcesUsed: resourceUsed,
		}
		queueReports = append(queueReports, &queueReport)
	}
	return queueReports
}

func (clusterUtilisationService *ClusterUtilisationService) getUsageByQueue(pods []*v1.Pod) map[string]common.ComputeResources {
	utilisationByQueue := make(map[string]common.ComputeResources)

	for _, pod := range pods {
		queue, present := pod.Labels[domain.Queue]
		if !present {
			log.Errorf("Pod %s found not belonging to a queue, not reporting its usage", pod.Name)
			continue
		}

		podUsage := clusterUtilisationService.queueUtilisationService.GetPodUtilisation(pod)

		if _, ok := utilisationByQueue[queue]; ok {
			utilisationByQueue[queue].Add(podUsage)
		} else {
			utilisationByQueue[queue] = podUsage
		}
	}

	return utilisationByQueue
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

func GetAllocationByQueue(pods []*v1.Pod) map[string]common.ComputeResources {
	utilisationByQueue := make(map[string]common.ComputeResources)

	for _, pod := range pods {
		queue, present := pod.Labels[domain.Queue]
		if !present {
			log.Errorf("Pod %s found not belonging to a queue, not reporting its allocation", pod.Name)
			continue
		}

		podAllocatedResourece := common.CalculateTotalResourceRequest([]*v1.Pod{pod})

		if _, ok := utilisationByQueue[queue]; ok {
			utilisationByQueue[queue].Add(podAllocatedResourece)
		} else {
			utilisationByQueue[queue] = podAllocatedResourece
		}
	}

	return utilisationByQueue
}
