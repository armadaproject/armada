package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/coordination/v1"
	"k8s.io/utils/pointer"

	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
)

const (
	lockName      = "armada-test-lock"
	lockNamespace = "armada-test-namespace"
	podName       = "armada-test-pod"
	otherPodName  = "armada-test-pod-2"
)

type State int64

const (
	NotLeader State = iota
	Leader
)

// Test becoming leader.  This test is slightly awkward as the K8s leader election code
// in client-go doesn't seem to have a mechanism for manipulating the internal clock.
// As a result, the test works as follows:
// * Set up a mock K8s client that updates the lease owener ever 100ms
// * Set up the client such that it checks for updates every 10ms
// * assert thart the state transitions are as expected
func TestK8sLeaderController_BecomingLeader(t *testing.T) {
	tests := map[string]struct {
		states         []State // states to be returned from the server.
		expectedStates []State // state transitions observed by the client
	}{
		"Always Leader": {
			states:         []State{Leader, Leader, Leader},
			expectedStates: []State{Leader},
		},
		"Become Leader after some time": {
			states:         []State{NotLeader, NotLeader, NotLeader, Leader},
			expectedStates: []State{Leader},
		},
		"Lose Leader": {
			states:         []State{Leader, Leader, NotLeader},
			expectedStates: []State{Leader, NotLeader},
		},
		"Flip Flopping": {
			states:         []State{Leader, NotLeader, Leader, NotLeader, Leader, NotLeader},
			expectedStates: []State{Leader, NotLeader, Leader, NotLeader, Leader, NotLeader},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Set up the mocks
			ctrl := gomock.NewController(t)
			client := schedulermocks.NewMockLeasesGetter(ctrl)
			leaseInterface := schedulermocks.NewMockLeaseInterface(ctrl)
			client.EXPECT().Leases(lockNamespace).Return(leaseInterface).AnyTimes()

			lease := &v1.Lease{}
			idx := 0
			// update holderIdentity every 200 milliseconds
			// the sleep here should be fine because the client is polling every 10 millis
			go func() {
				var holderIdentity string
				for idx < len(tc.states) {
					state := tc.states[idx]
					switch state {
					case Leader:
						holderIdentity = podName
					case NotLeader:
						holderIdentity = otherPodName
					}
					lease = &v1.Lease{
						Spec: v1.LeaseSpec{
							HolderIdentity: pointer.String(holderIdentity),
						},
					}
					idx++
					time.Sleep(200 * time.Millisecond)
				}
			}()

			leaseInterface.
				EXPECT().
				Get(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(arg0, arg1, arg2 interface{}) (*v1.Lease, error) {
					return lease, nil
				}).AnyTimes()

			leaseInterface.
				EXPECT().
				Update(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(arg0, arg1, arg2 interface{}) (*v1.Lease, error) {
					return lease, nil
				}).AnyTimes()

			// Run the test
			controller := NewKubernetesLeaderController(testLeaderConfig(), client)
			testListener := NewTestLeaseListener(controller)
			controller.listener = testListener
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			go func() {
				err := controller.Run(ctx)
				assert.ErrorIs(t, err, context.Canceled)
			}()

			// Loop that periodically checks to see if we have all the messages we expect
			shouldLoop := true
			var msgs []LeaderToken
			for shouldLoop {
				select {
				case <-ctx.Done():
					shouldLoop = false
				default:
					msgs = testListener.GetMessages()
					if len(msgs) == len(tc.expectedStates) {
						shouldLoop = false
					} else {
						time.Sleep(10 * time.Millisecond)
					}
				}
			}

			// Assert the results
			require.Equal(t, len(tc.expectedStates), len(testListener.tokens))
			for i, state := range tc.expectedStates {
				tok := testListener.tokens[i]
				validation := testListener.validations[i]
				switch state {
				case Leader:
					assert.True(t, tok.leader)
					assert.True(t, validation)
				case NotLeader:
					assert.False(t, tok.leader)
					assert.False(t, validation)
				}
			}

			// cancel the context to ensure we clean up the goroutine
			cancel()
		})
	}
}

func testLeaderConfig() schedulerconfig.LeaderConfig {
	return schedulerconfig.LeaderConfig{
		LeaseLockName:      lockName,
		LeaseLockNamespace: lockNamespace,
		LeaseDuration:      100 * time.Millisecond,
		RenewDeadline:      90 * time.Millisecond,
		RetryPeriod:        10 * time.Millisecond,
		PodName:            podName,
	}
}

// Captures the state transitions returned by the LeaderController
type TestLeaseListener struct {
	tokens      []LeaderToken
	validations []bool
	lc          LeaderController
	mutex       sync.Mutex
}

func NewTestLeaseListener(lc LeaderController) *TestLeaseListener {
	return &TestLeaseListener{
		lc:    lc,
		mutex: sync.Mutex{},
	}
}

func (t *TestLeaseListener) GetMessages() []LeaderToken {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return append([]LeaderToken(nil), t.tokens...)
}

func (t *TestLeaseListener) onStartedLeading(_ context.Context) {
	t.handleNewToken()
}

func (t *TestLeaseListener) onStoppedLeading() {
	t.handleNewToken()
}

func (t *TestLeaseListener) handleNewToken() {
	t.mutex.Lock()
	token := t.lc.GetToken()
	t.tokens = append(t.tokens, token)
	t.validations = append(t.validations, t.lc.ValidateToken(token))
	t.mutex.Unlock()
}
