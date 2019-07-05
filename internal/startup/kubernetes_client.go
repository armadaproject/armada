package startup

import (
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func LoadDefaultKubernetesClient() (kubernetes.Interface, error) {
	//TODO Add in a way to have in cluster vs normal kubernetes client
	//config, err := rest.InClusterConfig()
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()

	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}
