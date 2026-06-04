package leaderelection

import (
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/client"
)

type Mode string

const (
	ModeStandalone Mode = "standalone"
	ModeKubernetes Mode = "kubernetes"
)

func ParseMode(mode string) (Mode, error) {
	parsedMode := Mode(strings.ToLower(mode))
	if err := parsedMode.Validate(); err != nil {
		return "", err
	}
	return parsedMode, nil
}

func (m Mode) Validate() error {
	switch m {
	case ModeStandalone, ModeKubernetes:
		return nil
	default:
		return errors.Errorf("%s is not a valid leader mode", m)
	}
}

// Config holds the leader election configuration parameters.
type Config struct {
	// Mode specifies the leader election mode. Valid modes are "standalone" or "kubernetes".
	Mode Mode `validate:"required"`
	// LeaseLockName is the name of the Kubernetes Lock Object.
	LeaseLockName string
	// LeaseLockNamespace is the namespace of the Kubernetes Lock Object.
	LeaseLockNamespace string
	// LeaseDuration is how long the lease is held for.
	// Non-leaders must wait this long before trying to acquire the lease.
	LeaseDuration time.Duration
	// RenewDeadline is the duration that the acting leader will retry refreshing leadership before giving up.
	RenewDeadline time.Duration
	// RetryPeriod is the duration the LeaderElector clients should wait between tries of actions.
	RetryPeriod time.Duration
	// PodName is the name of the pod.
	PodName string
	// Connection details to the leader
	LeaderConnection client.ApiConnectionDetails
}

// MetricsOptions holds configuration options for leader election behavior.
type MetricsOptions struct {
	// MetricsPrefix is the prefix for Prometheus metrics.
	MetricsPrefix string
	// MarkLeadingInStandaloneMode specifies whether to mark the instance as leading in standalone mode.
	MarkLeadingInStandaloneMode bool
}

// CreateLeaderController creates a leader controller based on the provided config and options.
// It returns a LeaderController interface for managing leader election.
func CreateLeaderController(ctx *armadacontext.Context, config Config, metricsOptions *MetricsOptions) (LeaderController, error) {
	if err := config.Mode.Validate(); err != nil {
		return nil, err
	}
	switch config.Mode {
	case ModeStandalone:
		ctx.Infof("Running in standalone leader election mode")
		leaderController := NewStandaloneLeaderController()

		if metricsOptions != nil {
			leaderStatusMetrics := NewLeaderStatusMetricsCollector(metricsOptions.MetricsPrefix, config.PodName)
			if metricsOptions.MarkLeadingInStandaloneMode {
				leaderStatusMetrics.MarkAsLeading()
			}
			prometheus.MustRegister(leaderStatusMetrics)
		}

		return leaderController, nil

	case ModeKubernetes:
		ctx.Infof("Running in kubernetes leader election mode")
		clusterConfig, err := LoadClusterConfig(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating kubernetes client")
		}

		clientSet, err := kubernetes.NewForConfig(clusterConfig)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating kubernetes client")
		}

		leaderController := NewKubernetesLeaderController(config, clientSet.CoordinationV1())

		if metricsOptions != nil {
			leaderStatusMetrics := NewLeaderStatusMetricsCollector(metricsOptions.MetricsPrefix, config.PodName)
			leaderController.RegisterListener(leaderStatusMetrics)
			prometheus.MustRegister(leaderStatusMetrics)
		}

		return leaderController, nil

	default:
		// unreachable
		return nil, errors.Errorf("%s is not a valid leader mode", config.Mode)
	}
}

// LoadClusterConfig loads Kubernetes cluster configuration.
// It attempts to use in-cluster config first, falling back to kubeconfig if not running in a cluster.
func LoadClusterConfig(ctx *armadacontext.Context) (*rest.Config, error) {
	config, err := rest.InClusterConfig()
	if errors.Is(err, rest.ErrNotInCluster) {
		ctx.Info("Running with default client configuration")
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		overrides := &clientcmd.ConfigOverrides{}
		return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	}
	ctx.Info("Running with in cluster client configuration")
	return config, err
}
