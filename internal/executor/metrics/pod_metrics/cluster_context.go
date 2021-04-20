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
	"github.com/G-Research/armada/internal/executor/service"
)

const (
	leasedPhase       = "Leased"
	queueLabel        = "queue"
	phaseLabel        = "phase"
	resourceTypeLabel = "resourceType"
)

type ClusterContextMetrics struct {
	context                 context.ClusterContext
	utilisationService      service.UtilisationService
	queueUtilisationService service.PodUtilisationService

	knownQueues map[string]bool

	podCountTotal      *prometheus.CounterVec
	podCount           *prometheus.GaugeVec
	podResourceRequest *prometheus.GaugeVec
	podResourceUsage   *prometheus.GaugeVec

	nodeCount             prometheus.Gauge
	nodeAvailableResource *prometheus.GaugeVec
	nodeTotalResource     *prometheus.GaugeVec
}

func NewClusterContextMetrics(context context.ClusterContext, utilisationService service.UtilisationService, queueUtilisationService service.PodUtilisationService) *ClusterContextMetrics {
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
			[]string{queueLabel, phaseLabel}),

		podCount: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "job_pod",
				Help: "Pods in different phases by queue",
			},
			[]string{queueLabel, phaseLabel}),
		podResourceRequest: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "job_pod_resource_request",
				Help: "Pod resource requests in different phases by queue",
			},
			[]string{queueLabel, phaseLabel, resourceTypeLabel}),
		podResourceUsage: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "job_pod_resource_usage",
				Help: "Pod resource usage in different phases by queue",
			},
			[]string{queueLabel, phaseLabel, resourceTypeLabel}),
		nodeCount: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "available_node_count",
				Help: "Number of nodes available for Armada jobs",
			}),
		nodeAvailableResource: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "available_node_resource_allocatable",
				Help: "Resource allocatable on nodes available for Armada jobs",
			},
			[]string{resourceTypeLabel}),
		nodeTotalResource: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "available_node_resource_total",
				Help: "Total resource on nodes available for Armada jobs",
			},
			[]string{resourceTypeLabel}),
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

func (m *ClusterContextMetrics) UpdateMetrics() {
	pods, e := m.context.GetBatchPods()
	if e != nil {
		log.Errorf("Unable to update metrics: %v", e)
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
		queueMetric[phase].resourceUsage.Add(usage)
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
				m.podResourceRequest.WithLabelValues(queue, phase, resourceType).Set(common.QuantityAsFloat64(request))
			}
			for resourceType, usage := range phaseMetric.resourceUsage {
				m.podResourceUsage.WithLabelValues(queue, phase, resourceType).Set(common.QuantityAsFloat64(usage))
			}
			m.podCount.WithLabelValues(queue, phase).Set(phaseMetric.count)
		}
	}

	allAvailableProcessingNodes, err := m.utilisationService.GetAllAvailableProcessingNodes()
	if err != nil {
		log.Errorf("Failed to get required information to report cluster usage because %s", err)
		return
	}

	allocatableNodeResource, err := m.utilisationService.GetTotalAllocatableClusterCapacity()
	if err != nil {
		log.Errorf("Failed to get required information to report cluster usage because %s", err)
		return
	}
	availableNodeResource := *allocatableNodeResource
	totalNodeResource := common.CalculateTotalResource(allAvailableProcessingNodes)

	m.nodeCount.Set(float64(len(allAvailableProcessingNodes)))
	for resourceType, allocatable := range availableNodeResource {
		m.nodeAvailableResource.WithLabelValues(resourceType).Set(common.QuantityAsFloat64(allocatable))
	}

	for resourceType, allocatable := range totalNodeResource {
		m.nodeTotalResource.WithLabelValues(resourceType).Set(common.QuantityAsFloat64(allocatable))
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
