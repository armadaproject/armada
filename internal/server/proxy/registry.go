package proxy

import (
	"errors"
	"sync"
	"time"

	"github.com/armadaproject/armada/pkg/proxyapi"
)

// ErrExecutorNotConnected is returned when an exec request targets an executor
// that has no active ProxyControl stream.
var ErrExecutorNotConnected = errors.New("executor not connected")

// controlStreamEntry holds the state for one connected executor.
type controlStreamEntry struct {
	// send is used by the server to push ProxyControlCommands to the executor.
	send   chan *proxyapi.ProxyControlCommand
	cancel func() // cancels the context associated with this entry (and all its sessions)

	mu            sync.Mutex
	lastHeartbeat time.Time
}

// ExecutorRegistry tracks which executors currently have an active ProxyControl
// stream, and provides a mechanism to send ProxyControlCommands to them.
type ExecutorRegistry struct {
	mu      sync.RWMutex
	entries map[string]*controlStreamEntry

	// clock is injectable for testing heartbeat timeout logic.
	clock func() time.Time

	heartbeatTimeout time.Duration
	checkInterval    time.Duration
}

// NewExecutorRegistry creates an ExecutorRegistry with the given heartbeat
// timeout. Executors that haven't sent a heartbeat within timeout are
// deregistered automatically by a background goroutine (started by Run).
func NewExecutorRegistry(heartbeatTimeout, checkInterval time.Duration) *ExecutorRegistry {
	return &ExecutorRegistry{
		entries:          make(map[string]*controlStreamEntry),
		clock:            time.Now,
		heartbeatTimeout: heartbeatTimeout,
		checkInterval:    checkInterval,
	}
}

// Register adds (or replaces) an executor entry. If the executor was already
// registered, the old entry's context is cancelled before replacing it.
// The cancel function must cancel the context that owns this connection.
func (r *ExecutorRegistry) Register(executorID string, cancel func()) chan *proxyapi.ProxyControlCommand {
	r.mu.Lock()
	defer r.mu.Unlock()

	if old, ok := r.entries[executorID]; ok {
		old.cancel()
	}

	ch := make(chan *proxyapi.ProxyControlCommand, 32)
	r.entries[executorID] = &controlStreamEntry{
		send:          ch,
		cancel:        cancel,
		lastHeartbeat: r.clock(),
	}
	return ch
}

// Deregister removes an executor entry and cancels its context.
func (r *ExecutorRegistry) Deregister(executorID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deregisterLocked(executorID)
}

func (r *ExecutorRegistry) deregisterLocked(executorID string) {
	if e, ok := r.entries[executorID]; ok {
		e.cancel()
		delete(r.entries, executorID)
	}
}

// UpdateHeartbeat records that executorID just sent a heartbeat.
func (r *ExecutorRegistry) UpdateHeartbeat(executorID string) {
	r.mu.RLock()
	e, ok := r.entries[executorID]
	r.mu.RUnlock()
	if !ok {
		return
	}
	e.mu.Lock()
	e.lastHeartbeat = r.clock()
	e.mu.Unlock()
}

// SendExecRequest delivers a StartExecSession command to the executor's control
// stream. Returns ErrExecutorNotConnected if no entry exists.
func (r *ExecutorRegistry) SendExecRequest(executorID string, req *proxyapi.StartExecSession) error {
	r.mu.RLock()
	e, ok := r.entries[executorID]
	r.mu.RUnlock()
	if !ok {
		return ErrExecutorNotConnected
	}
	select {
	case e.send <- &proxyapi.ProxyControlCommand{
		Command: &proxyapi.ProxyControlCommand_StartExec{StartExec: req},
	}:
		return nil
	default:
		// Channel full — executor is too slow; treat as disconnected.
		return ErrExecutorNotConnected
	}
}

// RunHeartbeatChecker runs a background loop that deregisters executors whose
// last heartbeat is older than the heartbeat timeout. Runs until ctx is done.
func (r *ExecutorRegistry) RunHeartbeatChecker(done <-chan struct{}) {
	ticker := time.NewTicker(r.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			r.evictStale()
		}
	}
}

func (r *ExecutorRegistry) evictStale() {
	now := r.clock()
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, e := range r.entries {
		e.mu.Lock()
		age := now.Sub(e.lastHeartbeat)
		e.mu.Unlock()
		if age > r.heartbeatTimeout {
			r.deregisterLocked(id)
		}
	}
}
