package cluster

import (
	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type KubernetesClientProvider interface {
	ClientForUser(user string, groups []string) (kubernetes.Interface, error)
	Client() kubernetes.Interface
	ClientConfig() *rest.Config
}

type ConfigKubernetesClientProvider struct {
	restConfig       *rest.Config
	impersonateUsers bool
	client           kubernetes.Interface
}

func NewKubernetesClientProvider(impersonateUsers bool) (*ConfigKubernetesClientProvider, error) {
	config, err := loadConfig()
	if err != nil {
		return nil, err
	}

	config.Burst = 10000
	config.QPS = 10000

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return &ConfigKubernetesClientProvider{
			restConfig:       config,
			impersonateUsers: impersonateUsers,
			client:           client},
		nil
}

func (c *ConfigKubernetesClientProvider) Client() kubernetes.Interface {
	return c.client
}

func (c *ConfigKubernetesClientProvider) ClientConfig() *rest.Config {
	return c.restConfig
}

func (c *ConfigKubernetesClientProvider) ClientForUser(user string, groups []string) (kubernetes.Interface, error) {
	if !c.impersonateUsers {
		return c.client, nil
	}
	config := *c.restConfig // shallow copy of the config
	config.Impersonate = rest.ImpersonationConfig{UserName: user, Groups: groups}
	return kubernetes.NewForConfig(&config)
}

func loadConfig() (*rest.Config, error) {
	config, err := rest.InClusterConfig()
	if err == rest.ErrNotInCluster {
		log.Info("Running with default client configuration")
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		overrides := &clientcmd.ConfigOverrides{}
		return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	}
	log.Info("Running with in cluster client configuration")
	return config, err
}
