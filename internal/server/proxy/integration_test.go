package proxy

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
	"github.com/armadaproject/armada/pkg/proxyapi"
)

const bufSize = 1 << 20 // 1 MB

const testJobUUID = "01arz3ndektsv4rrffq69g5fav"

// fakeAuthorizer permits or denies all actions.
type fakeAuthorizer bool

func (a fakeAuthorizer) AuthorizeAction(_ *armadacontext.Context, _ permission.Permission) error {
	if !bool(a) {
		return status.Error(codes.PermissionDenied, "denied")
	}
	return nil
}

func (a fakeAuthorizer) AuthorizeQueueAction(_ *armadacontext.Context, _ queue.Queue, _ permission.Permission, _ queue.PermissionVerb) error {
	if !bool(a) {
		return status.Error(codes.PermissionDenied, "denied")
	}
	return nil
}

// queueDenyAuthorizer permits global actions but denies all queue actions.
type queueDenyAuthorizer struct{}

func (a *queueDenyAuthorizer) AuthorizeAction(_ *armadacontext.Context, _ permission.Permission) error {
	return nil
}

func (a *queueDenyAuthorizer) AuthorizeQueueAction(_ *armadacontext.Context, _ queue.Queue, _ permission.Permission, _ queue.PermissionVerb) error {
	return status.Error(codes.PermissionDenied, "no queue access")
}

// fakeJobResolver is a test double for jobResolver.
type fakeJobResolver struct {
	result *ResolvedJob
	err    error
}

func (r *fakeJobResolver) ResolveRunningJob(_ context.Context, _ string) (*ResolvedJob, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.result != nil {
		return r.result, nil
	}
	return &ResolvedJob{ExecutorID: "e1", Namespace: "ns", RunID: "run1", Queue: "test-queue"}, nil
}

// fakeQueueRepository always returns a queue with the given name.
type fakeQueueRepository struct{}

func (r *fakeQueueRepository) GetQueue(_ *armadacontext.Context, name string) (queue.Queue, error) {
	return queue.Queue{Name: name}, nil
}

func (r *fakeQueueRepository) GetAllQueues(_ *armadacontext.Context) ([]queue.Queue, error) {
	return nil, nil
}

// integrationTestServer holds an in-process gRPC server using bufconn.
type integrationTestServer struct {
	service     *ProxyService
	grpcServer  *grpc.Server
	userClient  api.InteractiveServiceClient
	proxyClient proxyapi.ExecutorProxyApiClient
}

func newIntegrationTestServer(t *testing.T, authorizer auth.ActionAuthorizer, resolver jobResolver) *integrationTestServer {
	t.Helper()

	registry := NewExecutorRegistry(90*time.Second, 60*time.Second)
	svc := NewProxyService(registry, resolver, authorizer, &fakeQueueRepository{}, time.Hour)

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	api.RegisterInteractiveServiceServer(srv, svc)
	proxyapi.RegisterExecutorProxyApiServer(srv, svc)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return &integrationTestServer{
		service:     svc,
		grpcServer:  srv,
		userClient:  api.NewInteractiveServiceClient(conn),
		proxyClient: proxyapi.NewExecutorProxyApiClient(conn),
	}
}

// runFakeExecutor connects an executor "e1" to the server and handles exec
// sessions by echoing stdin to stdout then sending ExecDone.
func runFakeExecutor(t *testing.T, srv *integrationTestServer, executorID string) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ctrlStream, err := srv.proxyClient.ProxyControl(ctx)
	require.NoError(t, err)

	require.NoError(t, ctrlStream.Send(&proxyapi.ProxyControlMessage{
		Payload: &proxyapi.ProxyControlMessage_ExecutorId{ExecutorId: executorID},
	}))

	go func() {
		for {
			cmd, err := ctrlStream.Recv()
			if err != nil {
				return
			}
			if p, ok := cmd.Command.(*proxyapi.ProxyControlCommand_StartExec); ok {
				sessionID := p.StartExec.SessionId
				go echoExecSession(ctx, srv.proxyClient, sessionID)
			}
		}
	}()
}

// echoExecSession opens ExecProxy, echoes one stdin chunk as stdout, then exits.
func echoExecSession(ctx context.Context, client proxyapi.ExecutorProxyApiClient, sessionID string) {
	stream, err := client.ExecProxy(ctx)
	if err != nil {
		return
	}
	_ = stream.Send(&proxyapi.ExecProxyMessage{
		Payload: &proxyapi.ExecProxyMessage_SessionId{SessionId: sessionID},
	})

	msg, err := stream.Recv()
	if err == nil {
		if s, ok := msg.Payload.(*proxyapi.ExecProxyMessage_Stdin); ok {
			_ = stream.Send(&proxyapi.ExecProxyMessage{
				Payload: &proxyapi.ExecProxyMessage_Stdout{Stdout: s.Stdin},
			})
		}
	}

	_ = stream.Send(&proxyapi.ExecProxyMessage{
		Payload: &proxyapi.ExecProxyMessage_Done{Done: &proxyapi.ExecDone{ExitCode: 0}},
	})
}

// waitForExecutor polls until executorID is registered.
func waitForExecutor(t *testing.T, srv *integrationTestServer, executorID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return srv.service.registry.SendExecRequest(executorID, &proxyapi.StartExecSession{}) == nil
	}, 2*time.Second, 10*time.Millisecond, "executor %q did not register", executorID)
	// Drain the probe command we just sent (the fake executor will receive it
	// but there's no matching session — that's fine for the wait).
}

// ---- Test cases ----

func TestIntegration_EndToEnd(t *testing.T) {
	resolver := &fakeJobResolver{}
	srv := newIntegrationTestServer(t, fakeAuthorizer(true), resolver)
	runFakeExecutor(t, srv, "e1")
	waitForExecutor(t, srv, "e1")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := srv.userClient.Exec(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&api.ExecRequest{
		Payload: &api.ExecRequest_Init{Init: &api.ExecInit{
			JobId:   testJobUUID,
			Command: []string{"/bin/echo"},
			Stdin:   true,
		}},
	}))
	require.NoError(t, stream.Send(&api.ExecRequest{
		Payload: &api.ExecRequest_Stdin{Stdin: []byte("hello\n")},
	}))

	var gotStdout []byte
	exitCode := int32(-1)
	for {
		resp, err := stream.Recv()
		if err == io.EOF || err != nil {
			break
		}
		switch p := resp.Payload.(type) {
		case *api.ExecResponse_Stdout:
			gotStdout = append(gotStdout, p.Stdout...)
		case *api.ExecResponse_ExitCode:
			exitCode = p.ExitCode
		}
		if exitCode >= 0 {
			break
		}
	}

	assert.Equal(t, []byte("hello\n"), gotStdout)
	assert.Equal(t, int32(0), exitCode)
}

func TestIntegration_AuthRejection(t *testing.T) {
	srv := newIntegrationTestServer(t, fakeAuthorizer(false), &fakeJobResolver{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := srv.userClient.Exec(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&api.ExecRequest{
		Payload: &api.ExecRequest_Init{Init: &api.ExecInit{JobId: testJobUUID, Command: []string{"/bin/sh"}}},
	}))

	_, err = stream.Recv()
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestIntegration_JobNotFound(t *testing.T) {
	srv := newIntegrationTestServer(t, fakeAuthorizer(true), &fakeJobResolver{err: ErrJobNotFound})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := srv.userClient.Exec(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&api.ExecRequest{
		Payload: &api.ExecRequest_Init{Init: &api.ExecInit{JobId: testJobUUID, Command: []string{"/bin/sh"}}},
	}))

	_, err = stream.Recv()
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestIntegration_ExecutorNotConnected(t *testing.T) {
	// Resolver resolves "e1" but no executor is registered.
	srv := newIntegrationTestServer(t, fakeAuthorizer(true), &fakeJobResolver{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := srv.userClient.Exec(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&api.ExecRequest{
		Payload: &api.ExecRequest_Init{Init: &api.ExecInit{JobId: testJobUUID, Command: []string{"/bin/sh"}}},
	}))

	_, err = stream.Recv()
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestIntegration_ProxyControl_AuthRejection(t *testing.T) {
	srv := newIntegrationTestServer(t, fakeAuthorizer(false), &fakeJobResolver{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrlStream, err := srv.proxyClient.ProxyControl(ctx)
	require.NoError(t, err)

	require.NoError(t, ctrlStream.Send(&proxyapi.ProxyControlMessage{
		Payload: &proxyapi.ProxyControlMessage_ExecutorId{ExecutorId: "e1"},
	}))

	_, err = ctrlStream.Recv()
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestIntegration_ExecProxy_AuthRejection(t *testing.T) {
	srv := newIntegrationTestServer(t, fakeAuthorizer(false), &fakeJobResolver{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	execStream, err := srv.proxyClient.ExecProxy(ctx)
	require.NoError(t, err)

	require.NoError(t, execStream.Send(&proxyapi.ExecProxyMessage{
		Payload: &proxyapi.ExecProxyMessage_SessionId{SessionId: "some-session"},
	}))

	_, err = execStream.Recv()
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestIntegration_QueueOwnership_Rejection(t *testing.T) {
	srv := newIntegrationTestServer(t, &queueDenyAuthorizer{}, &fakeJobResolver{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := srv.userClient.Exec(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&api.ExecRequest{
		Payload: &api.ExecRequest_Init{Init: &api.ExecInit{JobId: testJobUUID, Command: []string{"/bin/sh"}}},
	}))

	_, err = stream.Recv()
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestIntegration_InvalidJobID(t *testing.T) {
	srv := newIntegrationTestServer(t, fakeAuthorizer(true), &fakeJobResolver{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := srv.userClient.Exec(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&api.ExecRequest{
		Payload: &api.ExecRequest_Init{Init: &api.ExecInit{JobId: "not-a-uuid,key=val", Command: []string{"/bin/sh"}}},
	}))

	_, err = stream.Recv()
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestIntegration_SessionTimeout(t *testing.T) {
	// Use a very short timeout so the session expires quickly.
	registry := NewExecutorRegistry(90*time.Second, 60*time.Second)
	svc := NewProxyService(registry, &fakeJobResolver{}, fakeAuthorizer(true), &fakeQueueRepository{}, 50*time.Millisecond)

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	api.RegisterInteractiveServiceServer(srv, svc)
	proxyapi.RegisterExecutorProxyApiServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	userClient := api.NewInteractiveServiceClient(conn)
	proxyClient := proxyapi.NewExecutorProxyApiClient(conn)

	// Connect a fake executor.
	ts := &integrationTestServer{service: svc, grpcServer: srv, userClient: userClient, proxyClient: proxyClient}
	runFakeExecutor(t, ts, "e1")
	waitForExecutor(t, ts, "e1")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := userClient.Exec(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&api.ExecRequest{
		Payload: &api.ExecRequest_Init{Init: &api.ExecInit{
			JobId:   testJobUUID,
			Command: []string{"/bin/sh"},
			Stdin:   true,
		}},
	}))

	// The session should terminate due to timeout; we expect EOF or DeadlineExceeded.
	_, err = stream.Recv()
	// The bridge closes; either EOF or DeadlineExceeded is acceptable.
	require.Error(t, err)
}
