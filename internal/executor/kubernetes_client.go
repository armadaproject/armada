package executor

import (
	"github.com/G-Research/k8s-batch/internal/executor/configuration"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func CreateKubernetesClient(kubernetesConfig *configuration.KubernetesConfiguration) (kubernetes.Interface, error) {
	config, err := loadConfig(kubernetesConfig)

	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}

func loadConfig(kubernetesConfig *configuration.KubernetesConfiguration) (*rest.Config, error) {
	if kubernetesConfig.InClusterDeployment {
		return rest.InClusterConfig()
	} else if kubernetesConfig.ConfigLocation != "" {
		return clientcmd.BuildConfigFromFlags("", kubernetesConfig.ConfigLocation)
	} else {
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		overrides := &clientcmd.ConfigOverrides{}
		return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	}
}
