package pod_metrics

import (
	"github.com/google/martian/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/domain"
	"github.com/G-Research/armada/internal/executor/metrics"
	"github.com/G-Research/armada/internal/executor/service"
)

const (
	leasedPhase = "Leased"
	queueLabel  = "queue"
	phaseLabel  = "phase"
)

type ClusterContextMetrics struct {
	context                 context.ClusterContext
	utilisationService      service.UtilisationService
	queueUtilisationService service.PodUtilisationService

	knownQueues map[string]bool

	podCountTotal    *prometheus.CounterVec
	podCount         *prometheus.GaugeVec
	podCpuRequest    *prometheus.GaugeVec
	podCpuUsage      *prometheus.GaugeVec
	podMemoryRequest *prometheus.GaugeVec
	podMemoryUsage   *prometheus.GaugeVec

	nodeCount           prometheus.Gauge
	nodeCpuAvailable    prometheus.Gauge
	nodeCpuTotal        prometheus.Gauge
	nodeMemoryAvailable prometheus.Gauge
	nodeMemoryTotal     prometheus.Gauge
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
		podCpuRequest: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "job_pod_cpu_request",
				Help: "Pod cpu requests in different phases by queue",
			},
			[]string{queueLabel, phaseLabel}),
		podCpuUsage: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "job_pod_cpu_usage",
				Help: "Pod cpu usage in different phases by queue",
			},
			[]string{queueLabel, phaseLabel}),
		podMemoryRequest: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "job_pod_memory_request_bytes",
				Help: "Pod memory requests in different phases by queue",
			},
			[]string{queueLabel, phaseLabel}),
		podMemoryUsage: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "job_pod_memory_usage_bytes",
				Help: "Pod memory usage in different phases by queue",
			},
			[]string{queueLabel, phaseLabel}),
		nodeCount: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "available_node_count",
				Help: "Number of nodes available for Armada jobs",
			}),
		nodeCpuAvailable: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "available_node_allocatable_cpu",
				Help: "Number of cpus available for Armada jobs",
			}),
		nodeCpuTotal: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "available_node_total_cpu",
				Help: "Number of cpus on nodes available for Armada jobs",
			}),
		nodeMemoryAvailable: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "available_node_allocatable_memory_bytes",
				Help: "Memory available for Armada jobs",
			}),
		nodeMemoryTotal: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "available_node_total_memory_bytes",
				Help: "Memory on nodes available  for Armada jobs",
			}),
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
	cpuRequest    float64
	cpuUsage      float64
	memoryRequest float64
	memoryUsage   float64
	count         float64
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

		request := common.TotalResourceRequest(&pod.Spec).AsFloat()
		usage := m.queueUtilisationService.GetPodUtilisation(pod).AsFloat()

		queueMetric[phase].count++
		queueMetric[phase].memoryRequest += request[string(v1.ResourceMemory)]
		queueMetric[phase].memoryUsage += usage[string(v1.ResourceMemory)]
		queueMetric[phase].cpuRequest += request[string(v1.ResourceCPU)]
		queueMetric[phase].cpuUsage += usage[string(v1.ResourceCPU)]
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
			m.podCount.WithLabelValues(queue, phase).Set(phaseMetric.count)
			m.podCpuRequest.WithLabelValues(queue, phase).Set(phaseMetric.cpuRequest)
			m.podCpuUsage.WithLabelValues(queue, phase).Set(phaseMetric.cpuUsage)
			m.podMemoryRequest.WithLabelValues(queue, phase).Set(phaseMetric.memoryRequest)
			m.podMemoryUsage.WithLabelValues(queue, phase).Set(phaseMetric.memoryUsage)
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
	totalNodeResource := common.CalculateTotalResource(allAvailableProcessingNodes).AsFloat()
	availableNodeResource := allocatableNodeResource.AsFloat()

	m.nodeCount.Set(float64(len(allAvailableProcessingNodes)))
	m.nodeCpuAvailable.Set(availableNodeResource[string(v1.ResourceCPU)])
	m.nodeCpuTotal.Set(totalNodeResource[string(v1.ResourceCPU)])
	m.nodeMemoryAvailable.Set(availableNodeResource[string(v1.ResourceMemory)])
	m.nodeMemoryTotal.Set(totalNodeResource[string(v1.ResourceMemory)])
}

func createPodPhaseMetric() map[string]*podMetric {
	return map[string]*podMetric{
		leasedPhase:             {},
		string(v1.PodPending):   {},
		string(v1.PodRunning):   {},
		string(v1.PodSucceeded): {},
		string(v1.PodFailed):    {},
		string(v1.PodUnknown):   {},
	}
}
