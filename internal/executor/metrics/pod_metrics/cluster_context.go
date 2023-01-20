package pod_metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/tools/cache"

	"github.com/armadaproject/armada/internal/common"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/metrics"
	"github.com/armadaproject/armada/internal/executor/node"
	"github.com/armadaproject/armada/internal/executor/utilisation"
)

const (
	leasedPhase       = "Leased"
	queueLabel        = "queue"
	phaseLabel        = "phase"
	resourceTypeLabel = "resourceType"
	nodeTypeLabel     = "nodeType"
)

const (
	UnassignedNodeType = "unassigned"
	UnknownNodeType    = "unknown"
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
	nodeInfoService         node.NodeInfoService
	knownQueues             map[string]map[string]bool
	podCountTotal           *prometheus.CounterVec
}

func ExposeClusterContextMetrics(
	context context.ClusterContext,
	utilisationService utilisation.UtilisationService,
	queueUtilisationService utilisation.PodUtilisationService,
	nodeInfoService node.NodeInfoService,
) *ClusterContextMetrics {
	m := &ClusterContextMetrics{
		context:                 context,
		utilisationService:      utilisationService,
		queueUtilisationService: queueUtilisationService,
		nodeInfoService:         nodeInfoService,
		knownQueues:             map[string]map[string]bool{},
		podCountTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: metrics.ArmadaExecutorMetricsPrefix + "job_pod_total",
				Help: "Counter for pods in different phases by queue",
			},
			[]string{queueLabel, phaseLabel}),
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
	resourceRequest armadaresource.ComputeResources
	resourceUsage   armadaresource.ComputeResources
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

	nodes, err := m.context.GetNodes()
	if err != nil {
		log.Errorf("Failed to get required information to calculate node metrics because %s", err)
		recordInvalidMetrics(metrics, e)
		return
	}

	nodeGroupAllocationInfos, err := m.utilisationService.GetAllNodeGroupAllocationInfo()
	if err != nil {
		log.Errorf("Failed to get required information to calculate node metrics because %s", err)
		recordInvalidMetrics(metrics, e)
		return
	}

	nodeNameToNodeTypeMap := m.createNodeTypeLookup(nodes)

	podMetrics := map[string]map[string]map[string]*podMetric{}

	for _, pod := range pods {
		queue, present := pod.Labels[domain.Queue]
		if !present {
			continue
		}

		phase := string(pod.Status.Phase)
		if phase == "" {
			phase = leasedPhase
		}
		nodeType := getNodeTypePodIsRunningOn(pod, nodeNameToNodeTypeMap)

		_, ok := podMetrics[queue]
		if !ok {
			queueMetric := map[string]map[string]*podMetric{
				nodeType: createPodPhaseMetric(),
			}
			podMetrics[queue] = queueMetric
		}
		nodeTypeMetric, ok := podMetrics[queue][nodeType]
		if !ok {
			nodeTypeMetric = createPodPhaseMetric()
			podMetrics[queue][nodeType] = nodeTypeMetric
		}

		request := common.TotalPodResourceRequest(&pod.Spec)
		usage := m.queueUtilisationService.GetPodUtilisation(pod)

		nodeTypeMetric[phase].count++
		nodeTypeMetric[phase].resourceRequest.Add(request)
		nodeTypeMetric[phase].resourceUsage.Add(usage.CurrentUsage)
	}
	m.setEmptyMetrics(podMetrics)

	for queue, nodeTypeMetrics := range podMetrics {
		for nodeType, phaseMetrics := range nodeTypeMetrics {
			m.setKnownQueue(queue, nodeType)

			for phase, phaseMetric := range phaseMetrics {
				for resourceType, request := range phaseMetric.resourceRequest {
					metrics <- prometheus.MustNewConstMetric(podResourceRequestDesc, prometheus.GaugeValue,
						armadaresource.QuantityAsFloat64(request), queue, phase, resourceType, nodeType)
				}
				for resourceType, usage := range phaseMetric.resourceUsage {
					metrics <- prometheus.MustNewConstMetric(podResourceUsageDesc, prometheus.GaugeValue,
						armadaresource.QuantityAsFloat64(usage), queue, phase, resourceType, nodeType)
				}
				metrics <- prometheus.MustNewConstMetric(podCountDesc, prometheus.GaugeValue, phaseMetric.count, queue, phase, nodeType)
			}
		}
	}

	for _, nodeGroup := range nodeGroupAllocationInfos {
		metrics <- prometheus.MustNewConstMetric(nodeCountDesc, prometheus.GaugeValue, float64(len(nodeGroup.Nodes)), nodeGroup.NodeType.Id)
		for resourceType, allocatable := range nodeGroup.NodeGroupAllocatableCapacity {
			metrics <- prometheus.MustNewConstMetric(nodeAvailableResourceDesc,
				prometheus.GaugeValue, armadaresource.QuantityAsFloat64(allocatable), resourceType,
				nodeGroup.NodeType.Id)
		}

		for resourceType, total := range nodeGroup.NodeGroupCapacity {
			metrics <- prometheus.MustNewConstMetric(nodeTotalResourceDesc, prometheus.GaugeValue, armadaresource.QuantityAsFloat64(total), resourceType, nodeGroup.NodeType.Id)
		}
	}
}

func (m *ClusterContextMetrics) setKnownQueue(queue string, nodeType string) {
	_, exists := m.knownQueues[queue]
	if !exists {
		m.knownQueues = map[string]map[string]bool{
			queue: {},
		}
	}
	m.knownQueues[queue][nodeType] = true
}

func (m *ClusterContextMetrics) setEmptyMetrics(podMetrics map[string]map[string]map[string]*podMetric) {
	// reset metric for queues without pods
	for queue, nodeTypes := range m.knownQueues {
		_, exists := podMetrics[queue]
		if !exists {
			podMetrics[queue] = map[string]map[string]*podMetric{}
			for nodeType := range nodeTypes {
				podMetrics[queue][nodeType] = createPodPhaseMetric()
			}
		} else {
			for nodeType := range nodeTypes {
				_, exists := podMetrics[queue][nodeType]
				if !exists {
					podMetrics[queue][nodeType] = createPodPhaseMetric()
				}
			}
		}
	}
}

func (m *ClusterContextMetrics) createNodeTypeLookup(nodes []*v1.Node) map[string]string {
	result := map[string]string{}
	for _, n := range nodes {
		result[n.Name] = m.nodeInfoService.GetType(n).Id
	}
	return result
}

func getNodeTypePodIsRunningOn(pod *v1.Pod, nodeNameToNodeTypeMap map[string]string) string {
	if pod.Spec.NodeName == "" {
		return UnassignedNodeType
	}
	if nodeGroup, present := nodeNameToNodeTypeMap[pod.Spec.NodeName]; present {
		return nodeGroup
	}
	log.Warnf("Could not find node for pod (%s) node (%s)", pod.Name, pod.Spec.NodeName)
	return UnknownNodeType
}

func createPodPhaseMetric() map[string]*podMetric {
	zeroComputeResource := armadaresource.ComputeResources{
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
