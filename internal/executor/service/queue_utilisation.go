package service

import (
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metrics_server "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/domain"
)

type QueueUtilisationService interface {
	GetQueueUtilisationData() map[string]*UsageMetric
}

type MetricsServerQueueUtilisationService struct {
	clusterContext       context.ClusterContext
	metricsServerClient  metrics_server.Interface
	queueUtilisationData map[string]*UsageMetric
	knownQueues          map[string]bool
	dataAccessMutex      sync.Mutex
}

func NewMetricsServerQueueUtilisationService(clusterContext context.ClusterContext, metricsServerClient metrics_server.Interface) *MetricsServerQueueUtilisationService {
	return &MetricsServerQueueUtilisationService{
		clusterContext:      clusterContext,
		metricsServerClient: metricsServerClient,
		knownQueues:         map[string]bool{},
		dataAccessMutex:     sync.Mutex{},
	}
}

type UsageMetric struct {
	ResourceUsed common.ComputeResources
}

func (u *UsageMetric) DeepCopy() *UsageMetric {
	return &UsageMetric{
		ResourceUsed: u.ResourceUsed.DeepCopy(),
	}
}

func (q *MetricsServerQueueUtilisationService) GetQueueUtilisationData() map[string]*UsageMetric {
	q.dataAccessMutex.Lock()
	defer q.dataAccessMutex.Unlock()
	return deepCopy(q.queueUtilisationData)
}

func (q *MetricsServerQueueUtilisationService) updateQueueUtilisationData(metrics map[string]*UsageMetric) {
	q.dataAccessMutex.Lock()
	defer q.dataAccessMutex.Unlock()
	q.queueUtilisationData = metrics
}

func (q *MetricsServerQueueUtilisationService) RefreshUtilisationData() {
	allBatchPods, e := q.clusterContext.GetBatchPods()
	if e != nil {
		log.Errorf("Unable to refresh queue utilisation data: %v", e)
	}

	usage, err := q.getUsageMetricsForManagedPods()
	if err != nil {
		log.Errorf("Failed to get required information to update queue utilisation data because %s", err)
	}

	podMetrics := map[string]*UsageMetric{}
	nameToQueueMap := createPodNameToQueueMap(allBatchPods)
	nameToPhaseMap := createPodNameToPodPhaseMap(allBatchPods)
	for _, pod := range usage.Items {
		queue, present := nameToQueueMap[pod.Name]
		if !present {
			continue
		}
		phase, present := nameToPhaseMap[pod.Name]
		if !present || phase == v1.PodSucceeded || phase == v1.PodFailed {
			continue
		}
		queueMetric, present := podMetrics[queue]
		if !present {
			queueMetric = &UsageMetric{
				ResourceUsed: map[string]resource.Quantity{},
			}
			podMetrics[queue] = queueMetric
			q.knownQueues[queue] = true
		}

		totalResources := make(common.ComputeResources)
		for _, container := range pod.Containers {
			containerResource := common.FromResourceList(container.Usage)
			totalResources.Add(containerResource)
		}

		queueMetric.ResourceUsed.Add(totalResources)
	}

	// reset metric for queues without pods
	for q, _ := range q.knownQueues {
		_, exists := podMetrics[q]
		if !exists {
			podMetrics[q] = &UsageMetric{}
		}
	}

	q.updateQueueUtilisationData(podMetrics)
}

func (q *MetricsServerQueueUtilisationService) getUsageMetricsForManagedPods() (*v1beta1.PodMetricsList, error) {
	managedPodLabels := make([]string, 0, 1)
	managedPodLabels = append(managedPodLabels, domain.JobId)

	listOptions := metav1.ListOptions{
		LabelSelector: strings.Join(managedPodLabels, ","),
	}

	return q.metricsServerClient.MetricsV1beta1().PodMetricses(metav1.NamespaceAll).List(listOptions)
}

func createPodNameToQueueMap(allBatchPods []*v1.Pod) map[string]string {
	nameToQueueMap := make(map[string]string, len(allBatchPods))
	for _, pod := range allBatchPods {
		queue, present := pod.Labels[domain.Queue]
		if !present {
			continue
		}
		nameToQueueMap[pod.Name] = queue
	}
	return nameToQueueMap
}

func createPodNameToPodPhaseMap(allBatchPods []*v1.Pod) map[string]v1.PodPhase {
	nameToStatusMap := make(map[string]v1.PodPhase, len(allBatchPods))
	for _, pod := range allBatchPods {
		nameToStatusMap[pod.Name] = pod.Status.Phase
	}
	return nameToStatusMap
}

func deepCopy(metrics map[string]*UsageMetric) map[string]*UsageMetric {
	deepCopy := make(map[string]*UsageMetric, len(metrics))

	for key, value := range metrics {
		deepCopy[key] = value.DeepCopy()
	}

	return deepCopy
}
