package proxy

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/armadaproject/armada/pkg/proxyapi"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// newTestRegistry creates a registry with controllable clock.
func newTestRegistry() (*ExecutorRegistry, *mockClock) {
	clk := &mockClock{now: time.Now()}
	r := NewExecutorRegistry(90*time.Second, 60*time.Second)
	r.clock = clk.Now
	return r, clk
}

type mockClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *mockClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *mockClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestRegistry_RegisterAndSend(t *testing.T) {
	r, _ := newTestRegistry()

	ch := r.Register("e1", func() {})
	defer r.Deregister("e1")

	req := &proxyapi.StartExecSession{SessionId: "s1", JobId: "j1"}
	err := r.SendExecRequest("e1", req)
	require.NoError(t, err)

	select {
	case cmd := <-ch:
		got := cmd.Command.(*proxyapi.ProxyControlCommand_StartExec).StartExec
		assert.Equal(t, "s1", got.SessionId)
		assert.Equal(t, "j1", got.JobId)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for command")
	}
}

func TestRegistry_SendToUnknown(t *testing.T) {
	r, _ := newTestRegistry()

	err := r.SendExecRequest("unknown", &proxyapi.StartExecSession{})
	assert.ErrorIs(t, err, ErrExecutorNotConnected)
}

func TestRegistry_Deregister(t *testing.T) {
	r, _ := newTestRegistry()
	r.Register("e1", func() {})
	r.Deregister("e1")

	err := r.SendExecRequest("e1", &proxyapi.StartExecSession{})
	assert.ErrorIs(t, err, ErrExecutorNotConnected)
}

func TestRegistry_ReRegisterCancelsOld(t *testing.T) {
	r, _ := newTestRegistry()

	oldCancelled := make(chan struct{})
	_ = r.Register("e1", func() { close(oldCancelled) })

	// Re-register same executor → old cancel should be called.
	_ = r.Register("e1", func() {})
	defer r.Deregister("e1")

	select {
	case <-oldCancelled:
	case <-time.After(time.Second):
		t.Fatal("old cancel not called after re-registration")
	}
}

func TestRegistry_HeartbeatTimeout(t *testing.T) {
	r, clk := newTestRegistry()

	deregistered := make(chan struct{})
	r.Register("e1", func() { close(deregistered) })

	// Advance time past the 90s timeout without sending heartbeats.
	clk.Advance(91 * time.Second)
	r.evictStale()

	select {
	case <-deregistered:
	case <-time.After(time.Second):
		t.Fatal("executor was not evicted after heartbeat timeout")
	}

	err := r.SendExecRequest("e1", &proxyapi.StartExecSession{})
	assert.ErrorIs(t, err, ErrExecutorNotConnected)
}

func TestRegistry_HeartbeatUpdate_Prevents_Eviction(t *testing.T) {
	r, clk := newTestRegistry()

	r.Register("e1", func() {})
	defer r.Deregister("e1")

	// Advance halfway, send heartbeat, advance past original timeout.
	clk.Advance(45 * time.Second)
	r.UpdateHeartbeat("e1")
	clk.Advance(46 * time.Second) // only 46s since heartbeat
	r.evictStale()

	// Should still be connected.
	err := r.SendExecRequest("e1", &proxyapi.StartExecSession{})
	assert.NoError(t, err)
}

func TestRegistry_ConcurrentStress(t *testing.T) {
	r, _ := newTestRegistry()

	var wg sync.WaitGroup
	const goroutines = 50

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			r.Register("e1", func() {})
			_ = r.SendExecRequest("e1", &proxyapi.StartExecSession{})
			r.Deregister("e1")
		}()
	}
	wg.Wait()
}
