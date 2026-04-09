package introspection

import (
	"fmt"

	cluster "github.com/armadaproject/armada/internal/common/cluster"
	"github.com/armadaproject/armada/internal/server/configuration"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// implements KubeClientFactory by routing to a per-cluster k8s client based on the cluster name reported in JobRunDetails
type MultiClusterKubeFactory struct {
	clients map[string]kubernetes.Interface
}

// builds one k8s client per configured worker cluster
func NewMultiClusterKubeFactory(clusters []configuration.WorkerClusterConfig) (*MultiClusterKubeFactory, error) {
	clients := make(map[string]kubernetes.Interface, len(clusters))
	for _, c := range clusters {
		cfg, err := loadKubeconfigFromFile(c.KubeconfigPath, c.Context)
		if err != nil {
			return nil, fmt.Errorf("cluster %q: loading kubeconfig %q: %w", c.Name, c.KubeconfigPath, err)
		}
		client, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("cluster %q: creating kubernetes client: %w", c.Name, err)
		}
		clients[c.Name] = client
	}
	return &MultiClusterKubeFactory{clients: clients}, nil
}

func loadKubeconfigFromFile(path string, context string) (*rest.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if path != "" {
		rules.ExplicitPath = path
	}
	overrides := &clientcmd.ConfigOverrides{}
	if context != "" {
		overrides.CurrentContext = context
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
}

func (f *MultiClusterKubeFactory) Client(clusterName string) (kubernetes.Interface, error) {
	c, ok := f.clients[clusterName]
	if !ok {
		return nil, fmt.Errorf("no kubernetes client registered for cluster %q", clusterName)
	}
	return c, nil
}

func (f *MultiClusterKubeFactory) ClientForUser(clusterName string, user string, groups []string) (kubernetes.Interface, error) {
	// User impersonation not implemented for multi-cluster; return base client
	return f.Client(clusterName)
}

// ClusterListerRegistry holds per-cluster pod and node listers; populated by server.go after starting informers
type ClusterListerRegistry struct {
	pods  map[string]corev1listers.PodLister
	nodes map[string]corev1listers.NodeLister
}

func NewClusterListerRegistry() *ClusterListerRegistry {
	return &ClusterListerRegistry{
		pods:  make(map[string]corev1listers.PodLister),
		nodes: make(map[string]corev1listers.NodeLister),
	}
}

func (r *ClusterListerRegistry) Register(clusterName string, pods corev1listers.PodLister, nodes corev1listers.NodeLister) {
	r.pods[clusterName] = pods
	r.nodes[clusterName] = nodes
}

func (r *ClusterListerRegistry) PodLister(clusterName string) (corev1listers.PodLister, error) {
	l, ok := r.pods[clusterName]
	if !ok {
		return nil, fmt.Errorf("no pod lister registered for cluster %q", clusterName)
	}
	return l, nil
}

func (r *ClusterListerRegistry) NodeLister(clusterName string) (corev1listers.NodeLister, error) {
	l, ok := r.nodes[clusterName]
	if !ok {
		return nil, fmt.Errorf("no node lister registered for cluster %q", clusterName)
	}
	return l, nil
}

// SingleClusterKubeFactory is kept for local dev path where there is only 1 cluster and no kubeconfig files to manage
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