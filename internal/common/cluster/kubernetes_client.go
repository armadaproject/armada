package cluster

import (
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/flowcontrol"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
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

func NewKubernetesClientProvider(impersonateUsers bool, qps float32, burst int) (*ConfigKubernetesClientProvider, error) {
	if qps == 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "qps",
			Value:   qps,
			Message: "qps must be positive",
		})
	}
	if burst == 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "burst",
			Value:   burst,
			Message: "burst must be positive",
		})
	}

	restConfig, err := loadConfig()
	if err != nil {
		return nil, err
	}

	// Use a shared rate limiter for all clients created by this provider.
	// This limits the total number of concurrent calls across all clients to burst
	// and the total number of calls per second to qps.
	restConfig.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(qps, burst)

	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}

	return &ConfigKubernetesClientProvider{
			restConfig:       restConfig,
			impersonateUsers: impersonateUsers,
			client:           client,
		},
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
