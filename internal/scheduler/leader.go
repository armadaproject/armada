package scheduler

import (
	"context"
	"sync/atomic"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// LeaderController is an interface to be implemented by structs that control which scheduler is leader
type LeaderController interface {
	// GetToken returns a LeaderToken which allows you to determine if you are leader or not
	GetToken() LeaderToken
	// ValidateToken allows a caller to determine whether a previously obtained token is still valid.
	// Returns true if the token is a leader and false otherwise
	ValidateToken(tok LeaderToken) bool
	// Run starts the controller.  This is a blocking call which will return when the provided context is cancelled
	Run(ctx context.Context) error
}

// StandaloneLeaderController returns a token that always indicates you are leader
// This can be used when only a single instance of the scheduler is needed
type StandaloneLeaderController struct {
	token LeaderToken
}

func NewStandaloneLeaderController() *StandaloneLeaderController {
	return &StandaloneLeaderController{
		token: NewLeaderToken(),
	}
}

func (lc *StandaloneLeaderController) GetToken() LeaderToken {
	return lc.token
}

func (lc *StandaloneLeaderController) ValidateToken(tok LeaderToken) bool {
	if tok.leader {
		return lc.token.id == tok.id
	}
	return false
}

func (lc *StandaloneLeaderController) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		}
	}
}

// LeaseListener  allows clients to listen for lease events
type LeaseListener interface {
	// Called when the client has started leading
	onStartedLeading(context.Context)
	// Called when the client has stopped leading
	onStoppedLeading()
}

// KubernetesLeaderController uses the Kubernetes Leader election mechanism to determine who is leader
// This allows multiple instances of the scheduler to be run for HA.
type KubernetesLeaderController struct {
	client   coordinationv1client.LeasesGetter
	token    atomic.Value
	config   LeaderConfig
	listener LeaseListener
}

func NewKubernetesLeaderController(config LeaderConfig, client coordinationv1client.LeasesGetter) *KubernetesLeaderController {
	return &KubernetesLeaderController{
		client: client,
		token:  atomic.Value{},
		config: config,
	}
}

func (lc *KubernetesLeaderController) GetToken() LeaderToken {
	return lc.token.Load().(LeaderToken)
}

func (lc *KubernetesLeaderController) ValidateToken(tok LeaderToken) bool {
	if tok.leader {
		return lc.token.Load().(LeaderToken).id == tok.id
	}
	return false
}

// Run starts the controller.  This is a blocking call which will return when the provided context is cancelled
func (lc *KubernetesLeaderController) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			lock := lc.getNewLock()
			log.Infof("attempting to become leader")
			leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
				Lock:            lock,
				ReleaseOnCancel: true,
				LeaseDuration:   lc.config.LeaseDuration,
				RenewDeadline:   lc.config.RenewDeadline,
				RetryPeriod:     lc.config.RetryPeriod,
				Callbacks: leaderelection.LeaderCallbacks{
					OnStartedLeading: func(c context.Context) {
						log.Infof("I am now leader")
						lc.token.Store(NewLeaderToken())
						if lc.listener != nil {
							lc.listener.onStartedLeading(ctx)
						}
					},
					OnStoppedLeading: func() {
						log.Infof("I am no longer leader")
						lc.token.Store(InvalidLeaderToken())
						if lc.listener != nil {
							lc.listener.onStoppedLeading()
						}
					},
				},
			})
			log.Infof("leader election round finished")
		}
	}
}

// getNewLock returns a resourcelock.LeaseLock which is the resource used for locking when attempting leader election
func (lc *KubernetesLeaderController) getNewLock() *resourcelock.LeaseLock {
	return &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      lc.config.LeaseLockName,
			Namespace: lc.config.LeaseLockNamespace,
		},
		Client: lc.client,
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: lc.config.PodName,
		},
	}
}

// LeaderToken is a token handed out to schedulers which they can use to determine if they are leader
type LeaderToken struct {
	leader bool
	id     uuid.UUID
}

// InvalidLeaderToken returns a LeaderToken which indicates the scheduler is not leader
func InvalidLeaderToken() LeaderToken {
	return LeaderToken{
		leader: false,
		id:     uuid.New(),
	}
}

// NewLeaderToken returns a LeaderToken which indicates the scheduler is leader
func NewLeaderToken() LeaderToken {
	return LeaderToken{
		leader: true,
		id:     uuid.New(),
	}
}
