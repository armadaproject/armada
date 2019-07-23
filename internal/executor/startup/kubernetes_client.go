package startup

import (
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func CreateKubernetesClientWithDefaultConfig(inCluster bool) (kubernetes.Interface, error) {
	config, err := loadConfig(inCluster)

	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}

func loadConfig(inCluster bool) (*rest.Config, error) {
	if inCluster {
		return rest.InClusterConfig()
	} else {
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		overrides := &clientcmd.ConfigOverrides{}
		return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	}
}
