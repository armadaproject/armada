package joblogger

import (
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/armadaproject/armada/internal/common/context"

	pkgerrors "github.com/pkg/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

type Option func(logger *JobLogger)

type JobLogger struct {
	out        io.Writer
	clusters   []string
	interval   time.Duration
	jobSetId   string
	queue      string
	namespace  string
	podMap     sync.Map
	clientsets map[string]*kubernetes.Clientset
}

func New(clusters []string, namespace string, opts ...Option) (*JobLogger, error) {
	jl := &JobLogger{
		out:        os.Stdout,
		clusters:   clusters,
		namespace:  namespace,
		interval:   5 * time.Second,
		clientsets: make(map[string]*kubernetes.Clientset, len(clusters)),
	}

	for _, opt := range opts {
		opt(jl)
	}

	if err := jl.validateConfig(); err != nil {
		return nil, pkgerrors.WithMessage(err, "error validating supplied configuration")
	}

	return jl, nil
}

func WithOutput(output io.Writer) Option {
	return func(srv *JobLogger) {
		srv.out = output
	}
}

func WithJobSetId(jobSetId string) Option {
	return func(srv *JobLogger) {
		srv.jobSetId = jobSetId
	}
}

func WithQueue(queue string) Option {
	return func(srv *JobLogger) {
		srv.queue = queue
	}
}

func (srv *JobLogger) validateConfig() error {
	if srv.namespace == "" {
		return pkgerrors.New("namespace must be configured")
	}
	return nil
}

func (srv *JobLogger) Run(ctx *context.ArmadaContext) error {
	if len(srv.clusters) == 0 {
		return pkgerrors.New("no executor clusters configured to scrape for logs")
	}

	kubeconfig, err := getKubeConfigPath()
	if err != nil {
		return pkgerrors.WithMessage(err, "cannot find kubeconfig path")
	}

	if _, err := os.Stat(kubeconfig); err != nil {
		return pkgerrors.WithMessagef(err, "error checking does kubeconfig file exist at %s", kubeconfig)
	}

	g, ctx := context.ErrGroup(ctx)

	for i := range srv.clusters {
		kubectx := srv.clusters[i]
		clientset, err := newClientsetForKubectx(kubectx, kubeconfig)
		if err != nil {
			return pkgerrors.WithMessagef(err, "error creating clientset for kubectx %s", kubectx)
		}
		srv.clientsets[kubectx] = clientset
		g.Go(func() error {
			return srv.runWatcher(ctx, kubectx, clientset)
		})
	}

	g.Go(func() error {
		return srv.runScraper(ctx)
	})

	return g.Wait()
}

func getKubeConfigPath() (string, error) {
	if path, exists := os.LookupEnv("KUBECONFIG"); exists {
		return path, nil
	} else if home := homedir.HomeDir(); home != "" {
		return filepath.Join(home, ".kube", "config"), nil
	} else {
		return "", pkgerrors.New("neither $KUBECONFIG or $HOME envvars are configured")
	}
}

func buildConfigFromFlags(kubectx, kubeconfigPath string) (*rest.Config, error) {
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfigPath},
		&clientcmd.ConfigOverrides{
			CurrentContext: kubectx,
		}).ClientConfig()
}

func newClientsetForKubectx(kubectx, kubeconfig string) (*kubernetes.Clientset, error) {
	config, err := buildConfigFromFlags(kubectx, kubeconfig)
	if err != nil {
		return nil, pkgerrors.WithMessagef(err, "error creating config for kubecontext: %s", kubectx)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "error creating clientset")
	}

	return clientset, nil
}
