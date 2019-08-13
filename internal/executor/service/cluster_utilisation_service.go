package service

import (
	"context"
	"fmt"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/G-Research/k8s-batch/internal/executor/util"
	log "github.com/sirupsen/logrus"
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
	allAvailableProcessingNodes, err := clusterUtilisationService.getAllAvailableProcessingNodes()
	if err != nil {
		log.Errorf("Failed to get required information to report cluster usage because %s", err)
		return
	}

	totalNodeResource := common.CalculateTotalResource(allAvailableProcessingNodes)

	allActiveManagedPods, err := getAllActiveManagedPods(clusterUtilisationService.PodLister)
	if err != nil {
		log.Errorf("Failed to get required information to report cluster usage because %s", err)
		return
	}
	queueReports := createReportsOfQueueUsages(allActiveManagedPods)

	clusterUsage := api.ClusterUsageReport{
		ClusterId:       clusterUtilisationService.ClientId,
		ReportTime:      time.Now(),
		Queues:          queueReports,
		ClusterCapacity: totalNodeResource,
	}

	err = clusterUtilisationService.reportUsage(&clusterUsage)

	if err != nil {
		log.Errorf("Failed to report cluster usage because %s", err)
		return
	}
}

func (clusterUtilisationService ClusterUtilisationService) GetAvailableClusterCapacity() (*common.ComputeResources, error) {
	processingNodes, err := clusterUtilisationService.getAllAvailableProcessingNodes()
	if err != nil {
		return new(common.ComputeResources), fmt.Errorf("Failed getting available cluster capacity due to: %s", err)
	}

	allPods, err := clusterUtilisationService.PodLister.List(labels.Everything())
	if err != nil {
		return new(common.ComputeResources), fmt.Errorf("Failed getting available cluster capacity due to: %s", err)
	}

	podsOnProcessingNodes := getAllPodsOnNodes(allPods, processingNodes)
	activePodsOnProcessingNodes := util.FilterNonCompletedPods(podsOnProcessingNodes)

	totalNodeResource := common.CalculateTotalResource(processingNodes)
	totalPodResource := common.CalculateTotalResourceLimit(activePodsOnProcessingNodes)

	availableResource := totalNodeResource.DeepCopy()
	availableResource.Sub(totalPodResource)

	return &availableResource, nil
}

func (clusterUtilisationService ClusterUtilisationService) getAllAvailableProcessingNodes() ([]*v1.Node, error) {
	allNodes, err := clusterUtilisationService.NodeLister.List(labels.Everything())
	if err != nil {
		return []*v1.Node{}, err
	}

	return filterAvailableProcessingNodes(allNodes), nil
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

func getAllActiveManagedPods(podLister lister.PodLister) ([]*v1.Pod, error) {
	managedPodSelector := util.GetManagedPodSelector()
	allActiveManagedPods, err := podLister.List(managedPodSelector)
	if err != nil {
		return []*v1.Pod{}, err
	}
	allActiveManagedPods = util.FilterNonCompletedPods(allActiveManagedPods)
	return allActiveManagedPods, nil
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
		queue, present := pod.Labels[domain.Queue]
		if !present {
			log.Errorf("Pod %s found not belonging to a queue, not reporting its usage", pod.Name)
			continue
		}

		podComputeResource := common.CalculateTotalResourceLimit([]*v1.Pod{pod})

		if _, ok := utilisationByQueue[queue]; ok {
			utilisationByQueue[queue].Add(podComputeResource)
		} else {
			utilisationByQueue[queue] = podComputeResource
		}
	}

	return utilisationByQueue
}
