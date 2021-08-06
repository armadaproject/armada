package pod_metrics

import (
	"github.com/google/martian/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/tools/cache"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/domain"
	"github.com/G-Research/armada/internal/executor/metrics"
	"github.com/G-Research/armada/internal/executor/utilisation"
)

const (
	leasedPhase       = "Leased"
	queueLabel        = "queue"
	phaseLabel        = "phase"
	resourceTypeLabel = "resourceType"
	nodeTypeLabel     = "nodeType"
)

var podCountDesc = prometheus.NewDesc(
	metrics.ArmadaExecutorMetricsPrefix+"job_pod",
	"Pods in different phases by queue",
	[]string{queueLabel, phaseLabel, nodeTypeLabel}, nil,
)

var podResourceRequestDesc = prometheus.NewDesc(
	metrics.ArmadaExecutorMetricsPrefix+"job_pod_resource_request",
	"Pod resource requests in different phases by queue",
	[]string{queueLabel, phaseLabel, resourceTypeLabel, nodeTypeLabel}, nil,
)

var podResourceUsageDesc = prometheus.NewDesc(
	metrics.ArmadaExecutorMetricsPrefix+"job_pod_resource_usage",
	"Pod resource usage in different phases by queue",
	[]string{queueLabel, phaseLabel, resourceTypeLabel, nodeTypeLabel}, nil,
)

var nodeCountDesc = prometheus.NewDesc(
	metrics.ArmadaExecutorMetricsPrefix+"available_node_count",
	"Number of nodes available for Armada jobs",
	[]string{nodeTypeLabel}, nil,
)

var nodeAvailableResourceDesc = prometheus.NewDesc(
	metrics.ArmadaExecutorMetricsPrefix+"available_node_resource_allocatable",
	"Resource allocatable on nodes available for Armada jobs",
	[]string{resourceTypeLabel, nodeTypeLabel}, nil,
)

var nodeTotalResourceDesc = prometheus.NewDesc(
	metrics.ArmadaExecutorMetricsPrefix+"available_node_resource_total",
	"Total resource on nodes available for Armada jobs",
	[]string{resourceTypeLabel, nodeTypeLabel}, nil,
)

type ClusterContextMetrics struct {
	context                 context.ClusterContext
	utilisationService      utilisation.UtilisationService
	queueUtilisationService utilisation.PodUtilisationService

	knownQueues   map[string]bool
	podCountTotal *prometheus.CounterVec
}

func ExposeClusterContextMetrics(context context.ClusterContext, utilisationService utilisation.UtilisationService, queueUtilisationService utilisation.PodUtilisationService) *ClusterContextMetrics {
	m := &ClusterContextMetrics{
		context:                 context,
		utilisationService:      utilisationService,
		queueUtilisationService: queueUtilisationService,
		knownQueues:             map[string]bool{},
		podCountTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "job_pod_total",
				Help: "Counter for pods in different phases by queue",
			},
			[]string{queueLabel, phaseLabel, nodeTypeLabel}),
	}

	context.AddPodEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				return
			}
			m.reportPhase(pod)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldPod, ok1 := oldObj.(*v1.Pod)
			newPod, ok2 := newObj.(*v1.Pod)
			if !ok1 || !ok2 || oldPod.Status.Phase == newPod.Status.Phase {
				return
			}
			m.reportPhase(newPod)
		},
	})
	prometheus.MustRegister(m)
	return m
}

func (m *ClusterContextMetrics) reportPhase(pod *v1.Pod) {
	queue, present := pod.Labels[domain.Queue]
	if !present {
		return
	}
	m.podCountTotal.WithLabelValues(queue, string(pod.Status.Phase)).Inc()
}

type podMetric struct {
	resourceRequest common.ComputeResources
	resourceUsage   common.ComputeResources
	count           float64
}

func (m *ClusterContextMetrics) Describe(desc chan<- *prometheus.Desc) {
	desc <- podCountDesc
	desc <- podResourceRequestDesc
	desc <- podResourceUsageDesc
	desc <- nodeCountDesc
	desc <- nodeAvailableResourceDesc
	desc <- nodeTotalResourceDesc
}

func (m *ClusterContextMetrics) Collect(metrics chan<- prometheus.Metric) {
	pods, e := m.context.GetBatchPods()
	if e != nil {
		log.Errorf("Unable to get batch pods to calculate pod metrics because: %v", e)
		recordInvalidMetrics(metrics, e)
		return
	}

	allAvailableProcessingNodes, err := m.utilisationService.GetAllAvailableProcessingNodes()
	if err != nil {
		log.Errorf("Failed to get required information to calculate node metrics because %s", err)
		recordInvalidMetrics(metrics, e)
		return
	}

	allocatableNodeResource, err := m.utilisationService.GetAllocatableClusterResource()
	if err != nil {
		log.Errorf("Failed to get required information to calculate node metrics because %s", err)
		recordInvalidMetrics(metrics, e)
		return
	}

	podMetrics := map[string]map[string]*podMetric{}

	for _, pod := range pods {
		queue, present := pod.Labels[domain.Queue]
		if !present {
			continue
		}

		phase := string(pod.Status.Phase)
		if phase == "" {
			phase = leasedPhase
		}

		queueMetric, ok := podMetrics[queue]
		if !ok {
			queueMetric = createPodPhaseMetric()
			podMetrics[queue] = queueMetric
		}

		request := common.TotalPodResourceRequest(&pod.Spec)
		usage := m.queueUtilisationService.GetPodUtilisation(pod)

		queueMetric[phase].count++
		queueMetric[phase].resourceRequest.Add(request)
		queueMetric[phase].resourceUsage.Add(usage.CurrentUsage)
	}

	// reset metric for queues without pods
	for q, _ := range m.knownQueues {
		_, exists := podMetrics[q]
		if !exists {
			podMetrics[q] = createPodPhaseMetric()
		}
	}

	for queue, queueMetric := range podMetrics {
		m.knownQueues[queue] = true

		for phase, phaseMetric := range queueMetric {
			for resourceType, request := range phaseMetric.resourceRequest {
				metrics <- prometheus.MustNewConstMetric(podResourceRequestDesc, prometheus.GaugeValue,
					common.QuantityAsFloat64(request), queue, phase, resourceType)
			}
			for resourceType, usage := range phaseMetric.resourceUsage {
				metrics <- prometheus.MustNewConstMetric(podResourceUsageDesc, prometheus.GaugeValue,
					common.QuantityAsFloat64(usage), queue, phase, resourceType)
			}
			metrics <- prometheus.MustNewConstMetric(podCountDesc, prometheus.GaugeValue, phaseMetric.count, queue, phase)
		}
	}

	availableNodeResource := *allocatableNodeResource
	totalNodeResource := common.CalculateTotalResource(allAvailableProcessingNodes)

	metrics <- prometheus.MustNewConstMetric(nodeCountDesc, prometheus.GaugeValue, float64(len(allAvailableProcessingNodes)))
	for resourceType, allocatable := range availableNodeResource {
		metrics <- prometheus.MustNewConstMetric(nodeAvailableResourceDesc, prometheus.GaugeValue, common.QuantityAsFloat64(allocatable), resourceType)
	}

	for resourceType, total := range totalNodeResource {
		metrics <- prometheus.MustNewConstMetric(nodeTotalResourceDesc, prometheus.GaugeValue, common.QuantityAsFloat64(total), resourceType)
	}
}

func createPodPhaseMetric() map[string]*podMetric {
	zeroComputeResource := common.ComputeResources{
		"cpu":               resource.MustParse("0"),
		"memory":            resource.MustParse("0"),
		"ephemeral-storage": resource.MustParse("0"),
	}
	return map[string]*podMetric{
		leasedPhase:             {resourceRequest: zeroComputeResource.DeepCopy(), resourceUsage: zeroComputeResource.DeepCopy()},
		string(v1.PodPending):   {resourceRequest: zeroComputeResource.DeepCopy(), resourceUsage: zeroComputeResource.DeepCopy()},
		string(v1.PodRunning):   {resourceRequest: zeroComputeResource.DeepCopy(), resourceUsage: zeroComputeResource.DeepCopy()},
		string(v1.PodSucceeded): {resourceRequest: zeroComputeResource.DeepCopy(), resourceUsage: zeroComputeResource.DeepCopy()},
		string(v1.PodFailed):    {resourceRequest: zeroComputeResource.DeepCopy(), resourceUsage: zeroComputeResource.DeepCopy()},
		string(v1.PodUnknown):   {resourceRequest: zeroComputeResource.DeepCopy(), resourceUsage: zeroComputeResource.DeepCopy()},
	}
}

func recordInvalidMetrics(metrics chan<- prometheus.Metric, e error) {
	metrics <- prometheus.NewInvalidMetric(podCountDesc, e)
	metrics <- prometheus.NewInvalidMetric(podResourceRequestDesc, e)
	metrics <- prometheus.NewInvalidMetric(podResourceUsageDesc, e)
	metrics <- prometheus.NewInvalidMetric(nodeCountDesc, e)
	metrics <- prometheus.NewInvalidMetric(nodeAvailableResourceDesc, e)
	metrics <- prometheus.NewInvalidMetric(nodeTotalResourceDesc, e)
}
