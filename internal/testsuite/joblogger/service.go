package joblogger

import (
	"context"
	"flag"
	pkgerrors "github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"io"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Option func(logger *JobLogger)

type JobLogger struct {
	out        io.Writer
	kubectxs   []string
	interval   time.Duration
	jobSetId   string
	queue      string
	namespace  string
	podMap     sync.Map
	clientsets map[string]*kubernetes.Clientset
}

func New(kubectxs []string, opts ...Option) *JobLogger {
	jl := &JobLogger{
		out:        os.Stdout,
		kubectxs:   kubectxs,
		interval:   5 * time.Second,
		clientsets: make(map[string]*kubernetes.Clientset, len(kubectxs)),
	}

	for _, opt := range opts {
		opt(jl)
	}

	return jl
}

func WithOutput(output io.Writer) Option {
	return func(srv *JobLogger) {
		srv.out = output
	}
}

func WithNamespace(namespace string) Option {
	return func(srv *JobLogger) {
		srv.namespace = namespace
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

func (srv *JobLogger) Run(ctx context.Context) error {
	kubeconfig := getKubeConfigPath()
	if kubeconfig == nil {
		return pkgerrors.New("cannot find kubeconfig path")
	}

	g, ctx := errgroup.WithContext(ctx)

	for i := range srv.kubectxs {
		kubectx := srv.kubectxs[i]
		clientset, err := newClientsetForKubectx(kubectx, *kubeconfig)
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

func getKubeConfigPath() *string {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()
	return kubeconfig
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
