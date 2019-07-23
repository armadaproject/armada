package service

import (
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
	allActiveManagedPods := getAllActiveManagedPods(clusterUtilisationService.PodLister)
	queueReports := createReportsOfQueueUsages(allActiveManagedPods)

	allNodes, err := clusterUtilisationService.NodeLister.List(labels.Everything())
	if err != nil {
		fmt.Println("Error getting node information")
	}

	allAvailableProcessingNodes := getAllAvailableProcessingNodes(allNodes)
	totalNodeResource := calculateTotalResource(allAvailableProcessingNodes)

	clusterUsage := api.ClusterUsageReport{
		ClusterId:       clusterUtilisationService.ClientId,
		ReportTime:      time.Now(),
		Queues:          queueReports,
		ClusterCapacity: totalNodeResource,
	}

	if clusterUsage.ClusterId == "" {

	}

	//TODO Add back in when API side is ready
	//ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	//defer cancel()
	//_, err = clusterUtilisationService.UsageClient.ReportUsage(ctx, &clusterUsage)
	//
	//if err != nil {
	//	fmt.Printf("Failed to report cluster usage because %s", err)
	//}
}

func getAllActiveManagedPods(podLister lister.PodLister) []*v1.Pod {
	managedPodSelector, err := util.CreateLabelSelectorForManagedPods(false)
	if err != nil {
		//TODO Handle error case
	}

	allActiveManagedPods, err := podLister.List(managedPodSelector)
	allActiveManagedPods = removePodsInTerminalState(allActiveManagedPods)
	return allActiveManagedPods
}

func removePodsInTerminalState(pods []*v1.Pod) []*v1.Pod {
	activePods := make([]*v1.Pod, 0)

	for _, pod := range pods {
		if !util.IsInTerminalState(pod) {
			activePods = append(activePods, pod)
		}
	}

	return activePods
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
		podComputeResource := calculateTotalResourceLimit([]*v1.Pod{pod})

		if _, ok := utilisationByQueue[queue]; ok {
			utilisationByQueue[queue].Add(podComputeResource)
		} else {
			utilisationByQueue[queue] = podComputeResource
		}
	}

	return utilisationByQueue
}
