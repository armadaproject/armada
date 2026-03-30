package proxy

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/server/permissions"
	serverqueue "github.com/armadaproject/armada/internal/server/queue"
	"github.com/armadaproject/armada/pkg/api"
	clientqueue "github.com/armadaproject/armada/pkg/client/queue"
	"github.com/armadaproject/armada/pkg/proxyapi"
)

const (
	// heartbeatInterval is how often the executor sends heartbeats.
	heartbeatInterval = 30 * time.Second
	// sessionWaitTimeout is how long the server waits for the executor to open
	// an ExecProxy stream after sending StartExecSession.
	sessionWaitTimeout = 30 * time.Second
)

// pendingSession is created by the Exec handler while it waits for the executor
// to open the matching ExecProxy stream.
type pendingSession struct {
	ch chan ExecutorExecStream
}

// jobResolver is the interface used by ProxyService to look up running jobs.
// The production implementation is *JobResolver; tests can inject a fake.
type jobResolver interface {
	ResolveRunningJob(ctx context.Context, jobID string) (*ResolvedJob, error)
}

// ProxyService implements both ExecutorProxyApiServer and InteractiveServiceServer.
type ProxyService struct {
	registry        *ExecutorRegistry
	resolver        jobResolver
	authorizer      auth.ActionAuthorizer
	queueRepository serverqueue.ReadOnlyQueueRepository

	mu      sync.Mutex
	pending map[string]*pendingSession
}

// NewProxyService creates a ProxyService with the given dependencies.
func NewProxyService(
	registry *ExecutorRegistry,
	resolver jobResolver,
	authorizer auth.ActionAuthorizer,
	queueRepository serverqueue.ReadOnlyQueueRepository,
) *ProxyService {
	return &ProxyService{
		registry:        registry,
		resolver:        resolver,
		authorizer:      authorizer,
		queueRepository: queueRepository,
		pending:         make(map[string]*pendingSession),
	}
}

// ProxyControl handles the persistent control stream from an executor.
// It registers the executor, relays heartbeats, and deregisters on disconnect.
func (s *ProxyService) ProxyControl(stream proxyapi.ExecutorProxyApi_ProxyControlServer) error {
	// First message must be the executor identity.
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	executorID, ok := first.Payload.(*proxyapi.ProxyControlMessage_ExecutorId)
	if !ok {
		return status.Error(codes.InvalidArgument, "first ProxyControl message must be executor_id")
	}

	if err := s.authorizer.AuthorizeAction(armadacontext.FromGrpcCtx(stream.Context()), permissions.ExecuteJobs); err != nil {
		return status.Errorf(codes.PermissionDenied, "not authorized: %v", err)
	}

	// Derive a context that outlives the stream handler: we tie the executor's
	// session context to the gRPC stream context so that when the stream dies,
	// all sessions deriving from this context are cancelled.
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	sendCh := s.registry.Register(executorID.ExecutorId, cancel)
	defer s.registry.Deregister(executorID.ExecutorId)

	// Forward commands from the channel to the executor.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case cmd, ok := <-sendCh:
				if !ok {
					return
				}
				if err := stream.Send(cmd); err != nil {
					return
				}
			}
		}
	}()

	// Read heartbeats until the stream closes.
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if _, ok := msg.Payload.(*proxyapi.ProxyControlMessage_Heartbeat); ok {
			s.registry.UpdateHeartbeat(executorID.ExecutorId)
		}
	}
}

// ExecProxy is called by the executor when it has opened a session. The first
// message must contain the session_id; subsequent messages are data frames.
func (s *ProxyService) ExecProxy(stream proxyapi.ExecutorProxyApi_ExecProxyServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	sessionID, ok := first.Payload.(*proxyapi.ExecProxyMessage_SessionId)
	if !ok {
		return status.Error(codes.InvalidArgument, "first ExecProxy message must be session_id")
	}

	if err := s.authorizer.AuthorizeAction(armadacontext.FromGrpcCtx(stream.Context()), permissions.ExecuteJobs); err != nil {
		return status.Errorf(codes.PermissionDenied, "not authorized: %v", err)
	}

	s.mu.Lock()
	ps, found := s.pending[sessionID.SessionId]
	if found {
		delete(s.pending, sessionID.SessionId)
	}
	s.mu.Unlock()

	if !found {
		return status.Errorf(codes.NotFound, "no pending session %s", sessionID.SessionId)
	}

	// Deliver this stream to the waiting Exec handler.
	ps.ch <- stream

	// Block until the stream's context is done (the bridge owns the stream
	// lifecycle after delivering it).
	<-stream.Context().Done()
	return nil
}

// Exec is the user-facing handler. It authenticates the user, resolves the
// job to an executor, signals the executor to start a session, waits for the
// executor's ExecProxy stream, and bridges the two.
func (s *ProxyService) Exec(stream api.InteractiveService_ExecServer) error {
	ctx := armadacontext.FromGrpcCtx(stream.Context())

	// First message must be ExecInit.
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	init, ok := first.Payload.(*api.ExecRequest_Init)
	if !ok {
		return status.Error(codes.InvalidArgument, "first Exec message must be init")
	}

	// Authorization check.
	if err := s.authorizer.AuthorizeAction(ctx, permissions.ExecJob); err != nil {
		return status.Errorf(codes.PermissionDenied, "not authorized to exec into jobs: %v", err)
	}

	// Validate job ID is a UUID to prevent label selector injection.
	if _, err := uuid.Parse(init.Init.JobId); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid job ID: must be a UUID")
	}

	// Resolve the running job to an executor.
	resolved, err := s.resolver.ResolveRunningJob(ctx, init.Init.JobId)
	if err != nil {
		return status.Errorf(codes.NotFound, "cannot resolve job %s: %v", init.Init.JobId, err)
	}

	// Queue-level authorization: user must have watch permission on the job's queue.
	q, err := s.queueRepository.GetQueue(ctx, resolved.Queue)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to load queue")
	}
	if err := s.authorizer.AuthorizeQueueAction(ctx, q, permissions.ExecJob, clientqueue.PermissionVerbWatch); err != nil {
		return status.Errorf(codes.PermissionDenied, "not authorized to exec into jobs in queue %s", resolved.Queue)
	}

	sessionID := uuid.New().String()

	ctx.Infof("exec session started: principal=%s job=%s queue=%s executor=%s session=%s",
		auth.GetPrincipal(ctx).GetName(), init.Init.JobId, resolved.Queue, resolved.ExecutorID, sessionID)
	defer ctx.Infof("exec session ended: session=%s", sessionID)

	// Register a pending session so ExecProxy can find us.
	ps := &pendingSession{ch: make(chan ExecutorExecStream, 1)}
	s.mu.Lock()
	s.pending[sessionID] = ps
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pending, sessionID)
		s.mu.Unlock()
	}()

	// Tell the executor to start the session.
	if err := s.registry.SendExecRequest(resolved.ExecutorID, &proxyapi.StartExecSession{
		SessionId: sessionID,
		JobId:     init.Init.JobId,
		RunId:     resolved.RunID,
		Namespace: resolved.Namespace,
		Command:   init.Init.Command,
		Tty:       init.Init.Tty,
		Stdin:     init.Init.Stdin,
		Container: init.Init.Container,
		PodNumber: init.Init.PodNumber,
	}); err != nil {
		return status.Errorf(codes.Unavailable, "executor not connected: %v", err)
	}

	// Wait for the executor to open its ExecProxy stream.
	waitCtx, waitCancel := context.WithTimeout(ctx, sessionWaitTimeout)
	defer waitCancel()

	var execStream ExecutorExecStream
	select {
	case <-waitCtx.Done():
		return status.Error(codes.DeadlineExceeded, "timed out waiting for executor to connect")
	case execStream = <-ps.ch:
	}

	// Bridge until done.
	BridgeStreams(stream.Context(), stream, execStream)
	return nil
}
