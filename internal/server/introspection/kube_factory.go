package introspection

import (
	cluster "github.com/armadaproject/armada/internal/common/cluster"
	"k8s.io/client-go/kubernetes"
)

// this is implemented using the existing KubernetesClientProvider. In order to enable multicluster functionality rest.config should be used. pardon our dust,etc

type SingleClusterKubeFactory struct {
	provider cluster.KubernetesClientProvider
}

func NewSingleClusterKubeFactory(provider cluster.KubernetesClientProvider) *SingleClusterKubeFactory {
    return &SingleClusterKubeFactory{provider: provider}
}

func (f *SingleClusterKubeFactory) Client(clusterName string) (kubernetes.Interface, error) {
	return f.provider.Client(), nil
}

func (f *SingleClusterKubeFactory) ClientForUser(clusterName string, user string, groups []string) (kubernetes.Interface, error) {
	return f.provider.ClientForUser(user, groups)
}