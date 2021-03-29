package service

import (
	"sync"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	"github.com/G-Research/armada/internal/common"
	commonUtil "github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/domain"
	"github.com/G-Research/armada/internal/executor/util"
)

type PodUtilisationService interface {
	GetPodUtilisation(pod *v1.Pod) common.ComputeResources
}

type MetricsServerPodUtilisationService struct {
	clusterContext     context.ClusterContext
	podUtilisationData map[string]common.ComputeResources
	dataAccessMutex    sync.Mutex
}

func NewMetricsServerQueueUtilisationService(clusterContext context.ClusterContext) *MetricsServerPodUtilisationService {
	return &MetricsServerPodUtilisationService{
		clusterContext:     clusterContext,
		podUtilisationData: map[string]common.ComputeResources{},
		dataAccessMutex:    sync.Mutex{},
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

func (q *MetricsServerPodUtilisationService) updatePodUtilisation(name string, resources common.ComputeResources) {
	q.dataAccessMutex.Lock()
	defer q.dataAccessMutex.Unlock()
	q.podUtilisationData[name] = resources
}

func (q *MetricsServerPodUtilisationService) removeFinishedPods(podNames map[string]bool) {
	q.dataAccessMutex.Lock()
	defer q.dataAccessMutex.Unlock()
	for name := range q.podUtilisationData {
		if !podNames[name] {
			delete(q.podUtilisationData, name)
		}
	}
}

func (q *MetricsServerPodUtilisationService) RefreshUtilisationData() {
	nodes, err := q.clusterContext.GetNodes()
	if err != nil {
		log.Errorf("Failed to retrieve nodes from context: %s", err)
	}

	pods, err := q.clusterContext.GetActiveBatchPods()
	if err != nil {
		log.Errorf("Failed to retrieve pods from context: %s", err)
	}
	podNames := commonUtil.StringListToSet(util.ExtractNames(pods))

	summaries := make(chan *v1alpha1.Summary, len(nodes))
	wg := sync.WaitGroup{}
	for _, n := range nodes {
		wg.Add(1)
		go func(node *v1.Node) {
			summary, err := q.clusterContext.GetNodeStatsSummary(node)
			if err != nil {
				log.Errorf("Error when getting stats for node %s: %s", node.Name, err)
				wg.Done()
				return
			}
			summaries <- summary
			wg.Done()
		}(n)
	}
	go func() {
		wg.Wait()
		close(summaries)
	}()

	for s := range summaries {
		for _, pod := range s.Pods {
			if podNames[pod.PodRef.Name] {
				q.updatePodStats(pod)
			}
		}
	}

	q.removeFinishedPods(podNames)
}

func (q *MetricsServerPodUtilisationService) updatePodStats(podStats v1alpha1.PodStats) {

	resources := common.ComputeResources{}
	resources["cpu"] = *resource.NewScaledQuantity(int64(*podStats.CPU.UsageNanoCores), -9)
	resources["memory"] = *resource.NewQuantity(int64(*podStats.Memory.WorkingSetBytes), resource.BinarySI)
	resources["ephemeral-storage"] = *resource.NewQuantity(int64(*podStats.EphemeralStorage.UsedBytes), resource.BinarySI)

	var (
		acceleratorDutyCycles int64
		acceleratorUsedMemory int64
	)

	// add custom metrics for gpu
	for _, c := range podStats.Containers {
		for _, a := range c.Accelerators {
			acceleratorDutyCycles += int64(a.DutyCycle)
			acceleratorUsedMemory += int64(a.MemoryUsed)
		}
	}

	resources[domain.AcceleratorDutyCycle] = *resource.NewScaledQuantity(acceleratorDutyCycles, -2)
	resources[domain.AcceleratorMemory] = *resource.NewScaledQuantity(acceleratorUsedMemory, -2)
	q.updatePodUtilisation(podStats.PodRef.Name, resources)
}
