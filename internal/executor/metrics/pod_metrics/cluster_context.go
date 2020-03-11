package pod_metrics

import (
	"strings"

	"github.com/google/martian/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metrics_server "k8s.io/metrics/pkg/client/clientset/versioned"

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
	context             context.ClusterContext
	utilisationService  service.UtilisationService
	metricsServerClient metrics_server.Interface

	knownQueues map[string]bool

	podCountTotal    *prometheus.CounterVec
	podCount         *prometheus.GaugeVec
	podCpuRequest    *prometheus.GaugeVec
	podCpuUsage      *prometheus.GaugeVec
	podMemoryRequest *prometheus.GaugeVec
	podMemoryUsage   *prometheus.GaugeVec

	nodeCount           prometheus.Gauge
	nodeCpuAvailable    prometheus.Gauge
	nodeMemoryAvailable prometheus.Gauge
}

func NewClusterContextMetrics(context context.ClusterContext, metricsServerClient metrics_server.Interface, utilisationService service.UtilisationService) *ClusterContextMetrics {
	m := &ClusterContextMetrics{
		context:             context,
		utilisationService:  utilisationService,
		metricsServerClient: metricsServerClient,
		knownQueues:         map[string]bool{},
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
				Help: "Pod cpu usages by queue",
			},
			[]string{queueLabel}),
		podMemoryRequest: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "job_pod_memory_request_bytes",
				Help: "Pod memory requests in different phases by queue",
			},
			[]string{queueLabel, phaseLabel}),
		podMemoryUsage: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "job_pod_memory_usage_bytes",
				Help: "Pod memory usages by queue",
			},
			[]string{queueLabel}),
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
		nodeMemoryAvailable: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "available_node_allocatable_memory_bytes",
				Help: "Memory available for Armada jobs",
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
	cpu    float64
	memory float64
	count  float64
}

func (m *ClusterContextMetrics) UpdateBasicMetrics() {
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

		resources := common.TotalResourceRequest(&pod.Spec).AsFloat()

		queueMetric[phase].count++
		queueMetric[phase].memory += resources[string(v1.ResourceMemory)]
		queueMetric[phase].cpu += resources[string(v1.ResourceCPU)]
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
			m.podCpuRequest.WithLabelValues(queue, phase).Set(phaseMetric.cpu)
			m.podMemoryRequest.WithLabelValues(queue, phase).Set(phaseMetric.memory)
		}
	}

	allAvailableProcessingNodes, err := m.utilisationService.GetAllAvailableProcessingNodes()
	if err != nil {
		log.Errorf("Failed to get required information to report cluster usage because %s", err)
		return
	}
	totalNodeResource := common.CalculateTotalResource(allAvailableProcessingNodes).AsFloat()

	m.nodeCount.Set(float64(len(allAvailableProcessingNodes)))
	m.nodeCpuAvailable.Set(totalNodeResource[string(v1.ResourceCPU)])
	m.nodeMemoryAvailable.Set(totalNodeResource[string(v1.ResourceMemory)])
}

func (m *ClusterContextMetrics) UpdateUsageMetrics() {
	allBatchPods, e := m.context.GetBatchPods()
	if e != nil {
		log.Errorf("Unable to update usage metrics: %v", e)
	}

	usage, err := m.getUsageMetricsForManagedPods()
	if err != nil {
		log.Errorf("Failed to get required information to report pod usage metrics because %s", err)
	}

	podMetrics := map[string]*podMetric{}
	nameToQueueMap := createPodNameToQueueMap(allBatchPods)
	for _, pod := range usage.Items {
		queue, present := nameToQueueMap[pod.Name]
		if !present {
			continue
		}
		queueMetric, present := podMetrics[queue]
		if !present {
			queueMetric = &podMetric{}
			podMetrics[queue] = queueMetric
		}

		totalResources := make(common.ComputeResources)
		for _, container := range pod.Containers {
			containerResource := common.FromResourceList(container.Usage)
			totalResources.Add(containerResource)
		}

		resources := totalResources.AsFloat()
		queueMetric.memory += resources[string(v1.ResourceMemory)]
		queueMetric.cpu += resources[string(v1.ResourceCPU)]
	}

	//TODO Pods by phase? Exclude completed?

	// reset metric for queues without pods
	for q, _ := range m.knownQueues {
		_, exists := podMetrics[q]
		if !exists {
			podMetrics[q] = &podMetric{}
		}
	}
	// TODO CONCURRENT MAP ACCESS

	for queue, queueMetric := range podMetrics {
		m.podCpuUsage.WithLabelValues(queue).Set(queueMetric.cpu)
		m.podMemoryUsage.WithLabelValues(queue).Set(queueMetric.memory)
	}
}

func (m *ClusterContextMetrics) getUsageMetricsForManagedPods() (*v1beta1.PodMetricsList, error) {
	managedPodLabels := make([]string, 0, 1)
	managedPodLabels = append(managedPodLabels, domain.JobId)

	listOptions := metav1.ListOptions{
		LabelSelector: strings.Join(managedPodLabels, ","),
	}

	return m.metricsServerClient.MetricsV1beta1().PodMetricses(metav1.NamespaceAll).List(listOptions)
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
