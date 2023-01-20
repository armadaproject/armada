package utilisation

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	commonUtil "github.com/armadaproject/armada/internal/common/util"
	cluster_context "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/node"
	"github.com/armadaproject/armada/internal/executor/util"
)

const inactivePodGracePeriod = 3 * time.Minute

type PodUtilisationService interface {
	GetPodUtilisation(pod *v1.Pod) *domain.UtilisationData
}

type KubeletPodUtilisationService struct {
	clusterContext     cluster_context.ClusterContext
	nodeInfoService    node.NodeInfoService
	podUtilisationData map[string]*domain.UtilisationData
	dataAccessMutex    sync.Mutex
}

func NewMetricsServerQueueUtilisationService(
	clusterContext cluster_context.ClusterContext,
	nodeInfoService node.NodeInfoService,
) *KubeletPodUtilisationService {
	return &KubeletPodUtilisationService{
		clusterContext:     clusterContext,
		nodeInfoService:    nodeInfoService,
		podUtilisationData: map[string]*domain.UtilisationData{},
		dataAccessMutex:    sync.Mutex{},
	}
}

func (q *KubeletPodUtilisationService) GetPodUtilisation(pod *v1.Pod) *domain.UtilisationData {
	if pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
		return domain.EmptyUtilisationData()
	}
	q.dataAccessMutex.Lock()
	defer q.dataAccessMutex.Unlock()
	utilisation, present := q.podUtilisationData[pod.Name]
	if !present {
		return domain.EmptyUtilisationData()
	}
	return utilisation.DeepCopy()
}

func (q *KubeletPodUtilisationService) updatePodUtilisation(name string, utilisationData *domain.UtilisationData) {
	q.dataAccessMutex.Lock()
	defer q.dataAccessMutex.Unlock()
	q.podUtilisationData[name] = utilisationData
}

func (q *KubeletPodUtilisationService) removeFinishedPods(podNames map[string]bool) {
	q.dataAccessMutex.Lock()
	defer q.dataAccessMutex.Unlock()
	for name := range q.podUtilisationData {
		if !podNames[name] {
			delete(q.podUtilisationData, name)
		}
	}
}

func (q *KubeletPodUtilisationService) RefreshUtilisationData() {
	pods, err := q.clusterContext.GetActiveBatchPods()
	if err != nil {
		log.Errorf("Failed to retrieve pods from context: %s", err)
		return
	}

	allNodes, err := q.clusterContext.GetNodes()
	if err != nil {
		log.Errorf("Failed to retrieve nodes from context: %s", err)
		return
	}

	processingNodes, err := q.nodeInfoService.GetAllAvailableProcessingNodes()
	if err != nil {
		log.Errorf("Failed to retrieve processing nodes: %s", err)
		return
	}
	nonProcessingNodes := util.RemoveNodesFromList(allNodes, processingNodes)

	// Only process nodes jobs can run on + nodes jobs are actively running on (to catch Running jobs on cordoned nodes)
	nodes := getNodesHostingActiveManagedPods(pods, nonProcessingNodes)
	nodes = util.MergeNodeList(nodes, processingNodes)
	// Remove NotReady nodes, as it means the kubelet is unlikely to respond
	nodes = util.FilterNodes(nodes, util.IsReady)

	podNames := commonUtil.StringListToSet(util.ExtractNames(pods))

	summaries := make(chan *v1alpha1.Summary, len(nodes))
	wg := sync.WaitGroup{}
	for _, n := range nodes {
		wg.Add(1)
		go func(node *v1.Node) {
			defer wg.Done()
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*15)
			defer cancelFunc()
			summary, err := q.clusterContext.GetNodeStatsSummary(ctx, node)
			if err != nil {
				log.Warnf("Error when getting stats for node %s: %s", node.Name, err)
				return
			}
			summaries <- summary
		}(n)
	}
	go func() {
		wg.Wait()
		close(summaries)
	}()

	for s := range summaries {
		for _, pod := range s.Pods {
			if podNames[pod.PodRef.Name] {
				q.updatePodStats(&pod)
			}
		}
	}

	q.removeFinishedPods(podNames)
}

// We define an active pod as:
// - Any pod that is not Failed/Succeeded and doesn't have a deletion timestamp within the last inactivePodGracePeriod
//   - This rules out pods that get stuck in terminating for greater than inactivePodGracePeriod
//
// - Any pod that is Failed/Succeeded within the last inactivePodGracePeriod
//   - The kubelet stops reporting metrics for completed pods, just having a grace period to try catch any last metrics
func getNodesHostingActiveManagedPods(pods []*v1.Pod, nodes []*v1.Node) []*v1.Node {
	managedPods := util.FilterPods(pods, util.IsManagedPod)
	nodesWithActiveManagedPods := []*v1.Node{}
	for _, n := range nodes {
		podsOnNode := util.GetPodsOnNodes(managedPods, []*v1.Node{n})

		hasActivePod := false
		for _, pod := range podsOnNode {
			// Active pods not stuck terminating
			if !util.IsInTerminalState(pod) && (pod.DeletionTimestamp == nil || pod.DeletionTimestamp.Add(inactivePodGracePeriod).After(time.Now())) {
				hasActivePod = true
				break
			}
			// Recent completed pods
			lastStatusChange, err := util.LastStatusChange(pod)
			if util.IsInTerminalState(pod) && err == nil && lastStatusChange.Add(inactivePodGracePeriod).After(time.Now()) {
				hasActivePod = true
				break
			}
		}

		if hasActivePod {
			nodesWithActiveManagedPods = append(nodesWithActiveManagedPods, n)
		}
	}
	return nodesWithActiveManagedPods
}

func (q *KubeletPodUtilisationService) updatePodStats(podStats *v1alpha1.PodStats) {
	currentUsage := armadaresource.ComputeResources{}
	cumulativeUsage := armadaresource.ComputeResources{}

	if podStats.CPU != nil && podStats.CPU.UsageNanoCores != nil {
		currentUsage["cpu"] = *resource.NewScaledQuantity(int64(*podStats.CPU.UsageNanoCores), -9)
	}
	if podStats.CPU != nil && podStats.CPU.UsageCoreNanoSeconds != nil {
		cumulativeUsage["cpu"] = *resource.NewScaledQuantity(int64(*podStats.CPU.UsageCoreNanoSeconds), -9)
	}
	if podStats.Memory != nil && podStats.Memory.WorkingSetBytes != nil {
		currentUsage["memory"] = *resource.NewQuantity(int64(*podStats.Memory.WorkingSetBytes), resource.BinarySI)
	}
	if podStats.EphemeralStorage != nil && podStats.EphemeralStorage.UsedBytes != nil {
		currentUsage["ephemeral-storage"] = *resource.NewQuantity(int64(*podStats.EphemeralStorage.UsedBytes), resource.BinarySI)
	}

	var (
		acceleratorDutyCycles int64
		acceleratorUsedMemory int64
		accelerator           bool
	)

	// add custom metrics for gpu
	for _, c := range podStats.Containers {
		for _, a := range c.Accelerators {
			accelerator = true
			acceleratorDutyCycles += int64(a.DutyCycle)
			acceleratorUsedMemory += int64(a.MemoryUsed)
		}
	}

	if accelerator {
		currentUsage[domain.AcceleratorDutyCycle] = *resource.NewScaledQuantity(acceleratorDutyCycles, -2)
		currentUsage[domain.AcceleratorMemory] = *resource.NewScaledQuantity(acceleratorUsedMemory, -2)
	}

	utilisationData := &domain.UtilisationData{
		CurrentUsage:    currentUsage,
		CumulativeUsage: cumulativeUsage,
	}
	q.updatePodUtilisation(podStats.PodRef.Name, utilisationData)
}
