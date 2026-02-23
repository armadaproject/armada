package introspection

import (
	"github.com/armadaproject/armada/internal/server/introspection"
	"k8s.io/client-go/kubernetes"
)

type KubeClientFactory interface {
	Client(cluster string) (kubernetes.Interface, error)
	ClientForUser(cluster, user string, groups []string) (kubernetes.Interface, error)
}

func New(kube KubeClientFactory) *IntrospectionServer {
	return &IntrospectionServer{
		UnimplementedIntrospectionServer: &introspection.UnimplementedIntrospectionServer{},
		kube: kube,
	}
}