package utilisation

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	clusterContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
)

type podUtilisationKubeletMetrics struct{}

func newPodUtilisationKubeletMetrics() *podUtilisationKubeletMetrics {
	return &podUtilisationKubeletMetrics{}
}

func (m *podUtilisationKubeletMetrics) fetch(nodes []*v1.Node, podNameToUtilisationData map[string]*domain.UtilisationData, clusterContext clusterContext.ClusterContext) {
	summaries := make(chan *v1alpha1.Summary, len(nodes))
	wg := sync.WaitGroup{}
	for _, n := range nodes {
		wg.Add(1)
		go func(node *v1.Node) {
			defer wg.Done()
			ctx, cancelFunc := armadacontext.WithTimeout(armadacontext.Background(), time.Second*15)
			defer cancelFunc()
			summary, err := clusterContext.GetNodeStatsSummary(ctx, node)
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
		for _, podStats := range s.Pods {
			utilisationData, exists := podNameToUtilisationData[podStats.PodRef.Name]
			if exists {
				updatePodStats(&podStats, utilisationData)
			}
		}
	}
}

func updatePodStats(podStats *v1alpha1.PodStats, utilisationData *domain.UtilisationData) {
	if podStats.CPU != nil && podStats.CPU.UsageNanoCores != nil {
		utilisationData.CurrentUsage["cpu"] = *resource.NewScaledQuantity(int64(*podStats.CPU.UsageNanoCores), -9)
	}
	if podStats.CPU != nil && podStats.CPU.UsageCoreNanoSeconds != nil {
		utilisationData.CumulativeUsage["cpu"] = *resource.NewScaledQuantity(int64(*podStats.CPU.UsageCoreNanoSeconds), -9)
	}
	if podStats.Memory != nil && podStats.Memory.WorkingSetBytes != nil {
		utilisationData.CurrentUsage["memory"] = *resource.NewQuantity(int64(*podStats.Memory.WorkingSetBytes), resource.BinarySI)
	}
	if podStats.EphemeralStorage != nil && podStats.EphemeralStorage.UsedBytes != nil {
		utilisationData.CurrentUsage["ephemeral-storage"] = *resource.NewQuantity(int64(*podStats.EphemeralStorage.UsedBytes), resource.BinarySI)
	}
}
