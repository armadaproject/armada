package utilisation

import (
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/context"
)

type ClusterInfoService interface {
	GetClusterInfo() (*ClusterInfo, error)
}

type KubernetesNodeStatusService struct {
	clusterContext    context.ClusterContext
	trackedNodeLabels []string
	toleratedTaints   map[string]bool
}

func NewNodeUtilisationService(
	clusterContext context.ClusterContext,
	trackedNodeLabels []string,
	toleratedTaints []string,
) *KubernetesNodeStatusService {
	return &KubernetesNodeStatusService{
		clusterContext:    clusterContext,
		trackedNodeLabels: trackedNodeLabels,
		toleratedTaints:   util.StringListToSet(toleratedTaints),
	}
}

type NodeInfo struct {
	Node               *v1.Node
	NodeType           string
	ReadyForProcessing bool
	ManagedPods        []*v1.Pod
	UnmanagedPods      []*v1.Pod
}

type ClusterInfo struct {
	Nodes                   []*NodeInfo
	UnassignedManagedPods   []*v1.Pod
	UnassignedUnmanagedPods []*v1.Pod
}

func (k *KubernetesNodeStatusService) GetAllNodeStatus() ([]*NodeInfo, error) {
	return []*NodeInfo{}, nil
}
