package service

import (
	"context"
	"fmt"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/G-Research/k8s-batch/internal/executor/util"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	lister "k8s.io/client-go/listers/core/v1"
	"time"
)

type ClusterUtilisationService struct {
	ClientId    string
	PodLister   lister.PodLister
	NodeLister  lister.NodeLister
	UsageClient api.UsageClient
}

func (clusterUtilisationService ClusterUtilisationService) ReportClusterUtilisation() {
	allAvailableProcessingNodes := clusterUtilisationService.getAllAvailableProcessingNodes()
	totalNodeResource := util.CalculateTotalResource(allAvailableProcessingNodes)

	allActiveManagedPods := getAllActiveManagedPods(clusterUtilisationService.PodLister)
	queueReports := createReportsOfQueueUsages(allActiveManagedPods)

	clusterUsage := api.ClusterUsageReport{
		ClusterId:       clusterUtilisationService.ClientId,
		ReportTime:      time.Now(),
		Queues:          queueReports,
		ClusterCapacity: totalNodeResource,
	}

	err := clusterUtilisationService.reportUsage(&clusterUsage)

	if err != nil {
		fmt.Printf("Failed to report cluster usage because %s \n", err)
	}
}

func (clusterUtilisationService ClusterUtilisationService) GetAvailableClusterCapacity() *common.ComputeResources {
	processingNodes := clusterUtilisationService.getAllAvailableProcessingNodes()
	allPods, err := clusterUtilisationService.PodLister.List(labels.Everything())
	if err != nil {
		fmt.Println("Error getting pod information")
	}

	podsOnProcessingNodes := getAllPodsOnNodes(allPods, processingNodes)
	activePodsOnProcessingNodes := util.FilterCompletedPods(podsOnProcessingNodes)

	totalNodeResource := util.CalculateTotalResource(processingNodes)
	totalPodResource := util.CalculateTotalResourceLimit(activePodsOnProcessingNodes)

	availableResource := totalNodeResource.DeepCopy()
	availableResource.Sub(totalPodResource)

	return &availableResource
}

func (clusterUtilisationService ClusterUtilisationService) getAllAvailableProcessingNodes() []*v1.Node {
	allNodes, err := clusterUtilisationService.NodeLister.List(labels.Everything())
	if err != nil {
		fmt.Println("Error getting node information")
		return []*v1.Node{}
	}

	return filterAvailableProcessingNodes(allNodes)
}

func (clusterUtilisationService ClusterUtilisationService) reportUsage(clusterUsage *api.ClusterUsageReport) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := clusterUtilisationService.UsageClient.ReportUsage(ctx, clusterUsage)

	return err
}

func filterAvailableProcessingNodes(nodes []*v1.Node) []*v1.Node {
	processingNodes := make([]*v1.Node, 0, len(nodes))

	for _, node := range nodes {
		if isAvailableProcessingNode(node) {
			processingNodes = append(processingNodes, node)
		}
	}

	return processingNodes
}

func isAvailableProcessingNode(node *v1.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}

	noSchedule := false

	for _, taint := range node.Spec.Taints {
		if taint.Effect == v1.TaintEffectNoSchedule {
			noSchedule = true
			break
		}
	}

	if noSchedule {
		return false
	}

	return true
}

func getAllPodsOnNodes(pods []*v1.Pod, nodes []*v1.Node) []*v1.Pod {
	podsBelongingToNodes := make([]*v1.Pod, 0, len(pods))

	nodeMap := make(map[string]*v1.Node)
	for _, node := range nodes {
		nodeMap[node.Name] = node
	}

	for _, pod := range pods {
		if _, present := nodeMap[pod.Spec.NodeName]; present {
			podsBelongingToNodes = append(podsBelongingToNodes, pod)
		}
	}

	return podsBelongingToNodes
}

func getAllActiveManagedPods(podLister lister.PodLister) []*v1.Pod {
	managedPodSelector, err := util.CreateLabelSelectorForManagedPods()
	if err != nil {
		//TODO Handle error case
	}

	allActiveManagedPods, err := podLister.List(managedPodSelector)
	allActiveManagedPods = util.FilterRunningPods(allActiveManagedPods)
	return allActiveManagedPods
}

func createReportsOfQueueUsages(pods []*v1.Pod) []*api.QueueReport {
	usagesByQueue := getUsageByQueue(pods)

	queueReports := make([]*api.QueueReport, 0, len(usagesByQueue))

	for queueName, queueUsage := range usagesByQueue {
		queueReport := api.QueueReport{
			Name:      queueName,
			Resources: queueUsage,
		}
		queueReports = append(queueReports, &queueReport)
	}
	return queueReports
}

func getUsageByQueue(pods []*v1.Pod) map[string]common.ComputeResources {
	utilisationByQueue := make(map[string]common.ComputeResources)

	for _, pod := range pods {
		queue := pod.Labels[domain.Queue]
		podComputeResource := util.CalculateTotalResourceLimit([]*v1.Pod{pod})

		if _, ok := utilisationByQueue[queue]; ok {
			utilisationByQueue[queue].Add(podComputeResource)
		} else {
			utilisationByQueue[queue] = podComputeResource
		}
	}

	return utilisationByQueue
}
