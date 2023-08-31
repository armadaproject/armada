// Package grpcpool provides a pool of grpc clients
package grpcpool

import (
	"container/ring"
	"errors"
	"sync"
	"time"

	"github.com/armadaproject/armada/internal/common/context"

	"google.golang.org/grpc"
)

var (
	// ErrClosed is the error when the client pool is closed
	ErrClosed = errors.New("grpc pool: client pool is closed")
	// ErrTimeout is the error when the client pool timed out
	ErrTimeout = errors.New("grpc pool: client pool timed out")
	// ErrAlreadyClosed is the error when the client conn was already closed
	ErrAlreadyClosed = errors.New("grpc pool: the connection was already closed")
	// ErrFullPool is the error when the pool is already full
	ErrFullPool = errors.New("grpc pool: closing a ClientConn into a full pool")
)

// Factory is a function type creating a grpc client
type Factory func() (*grpc.ClientConn, error)

// FactoryWithContext is a function type creating a grpc client
// that accepts the context parameter that could be passed from
// Get or NewWithContext method.
type FactoryWithContext func(*context.ArmadaContext) (*grpc.ClientConn, error)

// Pool is the grpc client pool
type Pool struct {
	clients         *ring.Ring
	factory         FactoryWithContext
	idleTimeout     time.Duration
	maxLifeDuration time.Duration
	mu              sync.RWMutex
}

// ClientConn is the wrapper for a grpc client conn
type ClientConn struct {
	*grpc.ClientConn
	ccMutex       sync.Mutex
	pool          *Pool
	ring          *ring.Ring
	timeUsed      time.Time
	timeInitiated time.Time
	unhealthy     bool
}

// New creates a new clients pool with the given initial and maximum capacity,
// and the timeout for the idle clients. Returns an error if the initial
// clients could not be created
func New(factory Factory, init, capacity int, idleTimeout time.Duration,
	maxLifeDuration ...time.Duration,
) (*Pool, error) {
	return NewWithContext(context.Background(), func(ctx *context.ArmadaContext) (*grpc.ClientConn, error) { return factory() },
		init, capacity, idleTimeout, maxLifeDuration...)
}

// NewWithContext creates a new clients pool with the given initial and maximum
// capacity, and the timeout for the idle clients. The context parameter would
// be passed to the factory method during initialization. Returns an error if the
// initial clients could not be created.
func NewWithContext(ctx *context.ArmadaContext, factory FactoryWithContext, init, capacity int, idleTimeout time.Duration,
	maxLifeDuration ...time.Duration,
) (*Pool, error) {
	if capacity <= 0 {
		capacity = 1
	}
	if init < 0 {
		init = 0
	}
	if init > capacity {
		init = capacity
	}
	p := &Pool{
		clients:     ring.New(capacity),
		factory:     factory,
		idleTimeout: idleTimeout,
	}
	if len(maxLifeDuration) > 0 {
		p.maxLifeDuration = maxLifeDuration[0]
	}
	for i := 0; i < init; i++ {
		c, err := factory(ctx)
		if err != nil {
			return nil, err
		}

		p.clients.Value = &ClientConn{
			ClientConn:    c,
			pool:          p,
			ring:          p.clients,
			timeUsed:      time.Now(),
			timeInitiated: time.Now(),
		}

		p.clients = p.clients.Next()
	}
	// Fill the rest of the pool with empty clients
	for i := 0; i < capacity-init; i++ {
		p.clients.Value = &ClientConn{
			pool: p,
			ring: p.clients,
		}
		p.clients = p.clients.Next()
	}
	return p, nil
}

func (p *Pool) getClients() *ring.Ring {
	return p.clients
}

// Close empties the pool calling Close on all its clients.
// You can call Close while there are outstanding clients.
// The pool channel is then closed, and Get will not be allowed anymore
func (p *Pool) Close() {
	p.mu.Lock()
	clients := p.clients
	p.clients = nil
	p.mu.Unlock()

	if clients == nil {
		return
	}

	for i := 0; i < clients.Len(); i++ {
		if client, ok := clients.Value.(*ClientConn); ok {
			if client.ClientConn == nil {
				continue
			}
			client.ClientConn.Close()
		}
	}
}

// IsClosed returns true if the client pool is closed.
func (p *Pool) IsClosed() bool {
	return p == nil || p.getClients() == nil
}

// Get will return the next available client. If capacity
// has not been reached, it will create a new one using the factory.
func (p *Pool) Get(ctx *context.ArmadaContext) (*ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	clients := p.getClients()
	if clients == nil {
		return nil, ErrClosed
	}

	// Every time Get() is called, we advance to the next client in the ring.
	// This is essentially round-robin load-balancing. Keeping it simple for now.
	p.clients = p.clients.Next()

	client := p.clients.Value.(*ClientConn)

	// Make a new client if there's not one here in the ring already.
	if client == nil {
		client = &ClientConn{
			pool: p,
			ring: p.clients,
		}
		p.clients.Value = client
	}

	// If the wrapper was idle too long, close the connection and create a new
	// one. It's safe to assume that there isn't any newer client as the client
	// we fetched is the first in the channel
	idleTimeout := p.idleTimeout
	if client.ClientConn != nil && idleTimeout > 0 &&
		client.timeUsed.Add(idleTimeout).Before(time.Now()) {

		client.ClientConn.Close()
		client.dispose()
	}

	var err error
	var conn *grpc.ClientConn
	if client.ClientConn == nil ||
		client.unhealthy {
		conn, err = p.factory(ctx)
		if err != nil {
			return nil, err
		}
		client.revive(conn)
		client.unhealthy = false
		// This is a new connection, reset its initiated time
		client.timeInitiated = time.Now()
	}

	client.timeUsed = time.Now()

	// This way a user of ClientConn can call ClientConn.Close() and not
	// get to keep using the client.
	wrapper := &ClientConn{
		ClientConn:    client.ClientConn,
		pool:          client.pool,
		ring:          client.ring,
		timeUsed:      client.timeUsed,
		timeInitiated: client.timeInitiated,
		unhealthy:     client.unhealthy,
	}

	return wrapper, err
}

// Unhealthy marks the client conn as unhealthy, so that the connection
// gets reset when closed.
func (c *ClientConn) Unhealthy() {
	c.unhealthy = true
	// If this copy's grpc connection is unhealthy, so is the one held by
	// the pool's ring.
	// FIXME: Should we use a shared context instead?
	c.pool.mu.Lock()
	defer c.pool.mu.Unlock()
	client := c.ring.Value.(*ClientConn)
	if client != nil {
		client.unhealthy = true
	}
}

func (c *ClientConn) revive(conn *grpc.ClientConn) {
	c.ccMutex.Lock()
	defer c.ccMutex.Unlock()
	c.ClientConn = conn
}

func (c *ClientConn) dispose() {
	c.ccMutex.Lock()
	defer c.ccMutex.Unlock()
	c.ClientConn = nil
}

// Close 'returns' a ClientConn to the pool. It is safe to call multiple time,
// but will return an error after first time. Note that grpc connections can
// be safeuly utilized concurrently by many clients. Therefore there's no
// real need to grant exclusive use of a connection to one client at a time.
func (c *ClientConn) Close() error {
	if c == nil {
		return nil
	}
	if c.ClientConn == nil {
		return ErrAlreadyClosed
	}
	if c.pool.IsClosed() {
		return ErrClosed
	}
	// If the wrapper connection has become too old, we want to recycle it. To
	// clarify the logic: if the sum of the initialization time and the max
	// duration is before Now(), it means the initialization is so old adding
	// the maximum duration couldn't put in the future. This sum therefore
	// corresponds to the cut-off point: if it's in the future we still have
	// time, if it's in the past it's too old
	maxDuration := c.pool.maxLifeDuration
	if maxDuration > 0 && c.timeInitiated.Add(maxDuration).Before(time.Now()) {
		c.Unhealthy()
	}

	c.dispose()

	return nil
}

// Capacity returns the capacity
func (p *Pool) Capacity() int {
	if p.IsClosed() {
		return 0
	}
	return p.clients.Len()
}
