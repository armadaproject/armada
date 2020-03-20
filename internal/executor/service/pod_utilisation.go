package service

import (
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metrics_server "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/domain"
)

type PodUtilisationService interface {
	GetPodUtilisation(pod *v1.Pod) common.ComputeResources
}

type MetricsServerPodUtilisationService struct {
	clusterContext      context.ClusterContext
	metricsServerClient metrics_server.Interface
	podUtilisationData  map[string]common.ComputeResources
	dataAccessMutex     sync.Mutex
}

func NewMetricsServerQueueUtilisationService(clusterContext context.ClusterContext, metricsServerClient metrics_server.Interface) *MetricsServerPodUtilisationService {
	return &MetricsServerPodUtilisationService{
		clusterContext:      clusterContext,
		metricsServerClient: metricsServerClient,
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

func (q *MetricsServerPodUtilisationService) GetPodUtilisation(pod *v1.Pod) common.ComputeResources {
	if pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
		return common.ComputeResources{}
	}
	q.dataAccessMutex.Lock()
	defer q.dataAccessMutex.Unlock()
	utilisation, present := q.podUtilisationData[pod.Name]
	if !present {
		return common.ComputeResources{}
	}
	return utilisation.DeepCopy()
}

func (q *MetricsServerPodUtilisationService) updatePodUtilisationData(metrics map[string]common.ComputeResources) {
	q.dataAccessMutex.Lock()
	defer q.dataAccessMutex.Unlock()
	q.podUtilisationData = metrics
}

func (q *MetricsServerPodUtilisationService) RefreshUtilisationData() {
	usage, err := q.getUtilisationMetricsForManagedPods()
	if err != nil {
		log.Errorf("Failed to get required information to update pod utilisation data because %s", err)
	}

	podMetrics := make(map[string]common.ComputeResources)
	for _, pod := range usage.Items {
		totalResources := make(common.ComputeResources)
		for _, container := range pod.Containers {
			containerResource := common.FromResourceList(container.Usage)
			totalResources.Add(containerResource)
		}
		podMetrics[pod.Name] = totalResources
	}

	q.updatePodUtilisationData(podMetrics)
}

func (q *MetricsServerPodUtilisationService) getUtilisationMetricsForManagedPods() (*v1beta1.PodMetricsList, error) {
	managedPodLabels := make([]string, 0, 1)
	managedPodLabels = append(managedPodLabels, domain.JobId)

	listOptions := metav1.ListOptions{
		LabelSelector: strings.Join(managedPodLabels, ","),
	}

	return q.metricsServerClient.MetricsV1beta1().PodMetricses(metav1.NamespaceAll).List(listOptions)
}
