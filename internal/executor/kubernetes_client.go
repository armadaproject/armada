package executor

import (
	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/G-Research/armada/internal/executor/configuration"
)

func CreateKubernetesClient(kubernetesConfig *configuration.KubernetesConfiguration) (kubernetes.Interface, error) {
	config, err := loadConfig(kubernetesConfig)

	if err != nil {
		return nil, err
	}

	config.Burst = 10000
	config.QPS = 10000

	return kubernetes.NewForConfig(config)
}

func loadConfig(kubernetesConfig *configuration.KubernetesConfiguration) (*rest.Config, error) {
	if kubernetesConfig.InClusterDeployment {
		log.Info("Running with in cluster client configuration")
		return rest.InClusterConfig()
	} else if kubernetesConfig.KubernetesConfigLocation != "" {
		log.Infof("Running with custom client configuration from %s", kubernetesConfig.KubernetesConfigLocation)
		return clientcmd.BuildConfigFromFlags("", kubernetesConfig.KubernetesConfigLocation)
	} else {
		log.Info("Running with default client configuration")
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		overrides := &clientcmd.ConfigOverrides{}
		return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	}
}
