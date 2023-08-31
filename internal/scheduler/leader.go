package scheduler

import (
	gocontext "context"
	"sync"
	"sync/atomic"

	"github.com/armadaproject/armada/internal/common/context"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
)

// LeaderController is an interface to be implemented by structs that control which scheduler is leader
type LeaderController interface {
	// GetToken returns a LeaderToken which allows you to determine if you are leader or not
	GetToken() LeaderToken
	// ValidateToken allows a caller to determine whether a previously obtained token is still valid.
	// Returns true if the token is a leader and false otherwise
	ValidateToken(tok LeaderToken) bool
	// Run starts the controller.  This is a blocking call which will return when the provided context is cancelled
	Run(ctx *context.ArmadaContext) error
	// GetLeaderReport returns a report about the current leader
	GetLeaderReport() LeaderReport
}

type LeaderReport struct {
	IsCurrentProcessLeader bool
	LeaderName             string
}

// LeaderToken is a token handed out to schedulers which they can use to determine if they are leader
type LeaderToken struct {
	leader bool
	id     uuid.UUID
}

// InvalidLeaderToken returns a LeaderToken indicating this instance is not leader.
func InvalidLeaderToken() LeaderToken {
	return LeaderToken{
		leader: false,
		id:     uuid.New(),
	}
}

// NewLeaderToken returns a LeaderToken indicating this instance is the leader.
func NewLeaderToken() LeaderToken {
	return LeaderToken{
		leader: true,
		id:     uuid.New(),
	}
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

func (lc *StandaloneLeaderController) GetLeaderReport() LeaderReport {
	return LeaderReport{
		LeaderName:             "standalone",
		IsCurrentProcessLeader: true,
	}
}

func (lc *StandaloneLeaderController) ValidateToken(tok LeaderToken) bool {
	if tok.leader {
		return lc.token.id == tok.id
	}
	return false
}

func (lc *StandaloneLeaderController) Run(ctx *context.ArmadaContext) error {
	return nil
}

// LeaseListener allows clients to listen for lease events.
type LeaseListener interface {
	// Called when the client has started leading.
	onStartedLeading(*context.ArmadaContext)
	// Called when the client has stopped leading,
	onStoppedLeading()
}

// KubernetesLeaderController uses the Kubernetes leader election mechanism to determine who is leader.
// This allows multiple instances of the scheduler to be run for high availability.
//
// TODO: Move into package in common.
type KubernetesLeaderController struct {
	client            coordinationv1client.LeasesGetter
	token             atomic.Value
	config            schedulerconfig.LeaderConfig // TODO: Move necessary config into this struct.
	currentLeaderLock sync.Mutex
	currentLeader     string
	listeners         []LeaseListener
}

func NewKubernetesLeaderController(config schedulerconfig.LeaderConfig, client coordinationv1client.LeasesGetter) *KubernetesLeaderController {
	controller := &KubernetesLeaderController{
		client:            client,
		token:             atomic.Value{},
		currentLeaderLock: sync.Mutex{},
		config:            config,
	}
	controller.token.Store(InvalidLeaderToken())
	return controller
}

func (lc *KubernetesLeaderController) RegisterListener(listener LeaseListener) {
	lc.listeners = append(lc.listeners, listener)
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

// Run starts the controller.
// This is a blocking call that returns when the provided context is cancelled.
func (lc *KubernetesLeaderController) Run(ctx *context.ArmadaContext) error {
	log := ctxlogrus.Extract(ctx)
	log = log.WithField("service", "KubernetesLeaderController")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
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
					OnStartedLeading: func(c gocontext.Context) {
						log.Infof("I am now leader")
						lc.token.Store(NewLeaderToken())
						for _, listener := range lc.listeners {
							listener.onStartedLeading(ctx)
						}
					},
					OnStoppedLeading: func() {
						log.Infof("I am no longer leader")
						lc.token.Store(InvalidLeaderToken())
						for _, listener := range lc.listeners {
							listener.onStoppedLeading()
						}
					},
					OnNewLeader: func(identity string) {
						lc.currentLeaderLock.Lock()
						defer lc.currentLeaderLock.Unlock()
						lc.currentLeader = identity
					},
				},
			})
			log.Infof("leader election round finished")
		}
	}
}

func (lc *KubernetesLeaderController) GetLeaderReport() LeaderReport {
	lc.currentLeaderLock.Lock()
	defer lc.currentLeaderLock.Unlock()
	return LeaderReport{
		LeaderName:             lc.currentLeader,
		IsCurrentProcessLeader: lc.currentLeader == lc.config.PodName,
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
