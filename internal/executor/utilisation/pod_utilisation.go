package utilisation

import (
	"net/http"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	commonUtil "github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	clusterContext "github.com/armadaproject/armada/internal/executor/context"
	cluster_context "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/node"
	"github.com/armadaproject/armada/internal/executor/util"
)

const inactivePodGracePeriod = 3 * time.Minute

type PodUtilisationService interface {
	GetPodUtilisation(pod *v1.Pod) *domain.UtilisationData
}

type PodUtilisationServiceImpl struct {
	clusterContext     cluster_context.ClusterContext
	fetchers           []podUtilisationFetcher
	nodeInfoService    node.NodeInfoService
	podUtilisationData map[string]*domain.UtilisationData
	dataAccessMutex    sync.Mutex
}

type podUtilisationFetcher interface {
	fetch(nodes []*v1.Node, podNameToUtilisationData map[string]*domain.UtilisationData, clusterContext clusterContext.ClusterContext)
}

func NewPodUtilisationService(
	clusterContext cluster_context.ClusterContext,
	nodeInfoService node.NodeInfoService,
	customConfigs []configuration.CustomUsageMetrics,
	httpClient *http.Client,
) *PodUtilisationServiceImpl {
	fetchers := []podUtilisationFetcher{newPodUtilisationKubeletMetrics()}
	for _, customConfig := range customConfigs {
		fetchers = append(fetchers, newPodUtilisationCustomMetrics(httpClient, &customConfig))
	}

	return &PodUtilisationServiceImpl{
		clusterContext:     clusterContext,
		fetchers:           fetchers,
		nodeInfoService:    nodeInfoService,
		podUtilisationData: map[string]*domain.UtilisationData{},
		dataAccessMutex:    sync.Mutex{},
	}
}

func (q *PodUtilisationServiceImpl) GetPodUtilisation(pod *v1.Pod) *domain.UtilisationData {
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

func (q *PodUtilisationServiceImpl) updatePodUtilisation(name string, utilisationData *domain.UtilisationData) {
	q.dataAccessMutex.Lock()
	defer q.dataAccessMutex.Unlock()
	q.podUtilisationData[name] = utilisationData
}

func (q *PodUtilisationServiceImpl) removeFinishedPods(podNames map[string]bool) {
	q.dataAccessMutex.Lock()
	defer q.dataAccessMutex.Unlock()
	for name := range q.podUtilisationData {
		if !podNames[name] {
			delete(q.podUtilisationData, name)
		}
	}
}

func (q *PodUtilisationServiceImpl) RefreshUtilisationData() {
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

	podNames := util.ExtractNames(pods)

	podNameToUtilisationData := map[string]*domain.UtilisationData{}
	for _, podName := range podNames {
		podNameToUtilisationData[podName] = &domain.UtilisationData{
			CurrentUsage:    armadaresource.ComputeResources{},
			CumulativeUsage: armadaresource.ComputeResources{},
		}
	}

	for _, fetcher := range q.fetchers {
		fetcher.fetch(nodes, podNameToUtilisationData, q.clusterContext)
	}

	for podName, utilisationData := range podNameToUtilisationData {
		q.updatePodUtilisation(podName, utilisationData)
	}

	q.removeFinishedPods(commonUtil.StringListToSet(podNames))
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
