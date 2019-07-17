package task

import (
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/G-Research/k8s-batch/internal/executor/service"
	v1 "k8s.io/api/core/v1"
	lister "k8s.io/client-go/listers/core/v1"
	"time"
)

type ClusterUtilisationReporterTask struct {
	PodLister lister.PodLister
	Interval  time.Duration
	//TODO API
}

func (clusterUtilisationReporter ClusterUtilisationReporterTask) Execute() {
	allActivePods := getAllActivePods(clusterUtilisationReporter.PodLister)
	getUtilisationByQueue(allActivePods)

}

func getAllActivePods(podLister lister.PodLister) []*v1.Pod {
	runningPodsSelector, err := service.CreateLabelSelectorForManagedPods(false)
	if err != nil {
		//TODO Handle error case
	}

	allActiveBatchPods, err := podLister.List(runningPodsSelector)
	allActiveBatchPods = removePodsInTerminalState(allActiveBatchPods)
	return allActiveBatchPods
}

func removePodsInTerminalState(pods []*v1.Pod) []*v1.Pod {
	activePods := make([]*v1.Pod, 0)

	for _, pod := range pods {
		if !service.IsInTerminalState(pod) {
			activePods = append(activePods, pod)
		}
	}

	return activePods
}

func getUtilisationByQueue(pods []*v1.Pod) map[string]v1.ResourceList {
	utilisationByQueue := make(map[string]v1.ResourceList)

	for _, pod := range pods {
		queue := pod.Labels[domain.Queue]

		if _, ok := utilisationByQueue[queue]; ok {
			//TODO Once we have decided upon which resource struct to use + manipulation methods implemented, add assignment code here
		} else {
			//TODO Once we have decided upon which resource struct to use + manipulation methods implemented, add assignment code here
		}
	}

	return utilisationByQueue
}

func (clusterUtilisationReporter ClusterUtilisationReporterTask) GetInterval() time.Duration {
	return clusterUtilisationReporter.Interval
}
