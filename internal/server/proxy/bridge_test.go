package proxy

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/proxyapi"
)

// mockUserStream is a fake UserExecStream.
type mockUserStream struct {
	ctx     context.Context
	recvCh  chan *api.ExecRequest
	sentMu  chan *api.ExecResponse
	recvErr error
}

func newMockUserStream(ctx context.Context) *mockUserStream {
	return &mockUserStream{
		ctx:    ctx,
		recvCh: make(chan *api.ExecRequest, 64),
		sentMu: make(chan *api.ExecResponse, 64),
	}
}

func (m *mockUserStream) Send(r *api.ExecResponse) error    { m.sentMu <- r; return nil }
func (m *mockUserStream) Context() context.Context          { return m.ctx }
func (m *mockUserStream) Recv() (*api.ExecRequest, error) {
	select {
	case r, ok := <-m.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return r, nil
	case <-m.ctx.Done():
		return nil, io.EOF
	}
}

// mockExecStream is a fake ExecutorExecStream.
type mockExecStream struct {
	ctx    context.Context
	recvCh chan *proxyapi.ExecProxyMessage
	sentCh chan *proxyapi.ExecProxyMessage
}

func newMockExecStream(ctx context.Context) *mockExecStream {
	return &mockExecStream{
		ctx:    ctx,
		recvCh: make(chan *proxyapi.ExecProxyMessage, 64),
		sentCh: make(chan *proxyapi.ExecProxyMessage, 64),
	}
}

func (m *mockExecStream) Send(r *proxyapi.ExecProxyMessage) error { m.sentCh <- r; return nil }
func (m *mockExecStream) Context() context.Context                { return m.ctx }
func (m *mockExecStream) Recv() (*proxyapi.ExecProxyMessage, error) {
	select {
	case r, ok := <-m.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return r, nil
	case <-m.ctx.Done():
		return nil, io.EOF
	}
}

func TestBridge_UserToExecutor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	user := newMockUserStream(ctx)
	exec := newMockExecStream(ctx)

	payload := bytes.Repeat([]byte("x"), 1024)
	user.recvCh <- &api.ExecRequest{Payload: &api.ExecRequest_Stdin{Stdin: payload}}
	close(user.recvCh)

	// Exec sends done to terminate bridge.
	exec.recvCh <- &proxyapi.ExecProxyMessage{Payload: &proxyapi.ExecProxyMessage_Done{Done: &proxyapi.ExecDone{ExitCode: 0}}}

	BridgeStreams(ctx, user, exec)

	// Check that stdin was forwarded.
	select {
	case msg := <-exec.sentCh:
		got := msg.Payload.(*proxyapi.ExecProxyMessage_Stdin).Stdin
		assert.Equal(t, payload, got)
	default:
		t.Fatal("no message forwarded to executor")
	}
}

func TestBridge_ExecutorToUser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	user := newMockUserStream(ctx)
	exec := newMockExecStream(ctx)

	payload := bytes.Repeat([]byte("y"), 1024)
	exec.recvCh <- &proxyapi.ExecProxyMessage{Payload: &proxyapi.ExecProxyMessage_Stdout{Stdout: payload}}
	exec.recvCh <- &proxyapi.ExecProxyMessage{Payload: &proxyapi.ExecProxyMessage_Done{Done: &proxyapi.ExecDone{ExitCode: 0}}}

	// Keep user recv open until bridge is done.
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(user.recvCh)
	}()

	BridgeStreams(ctx, user, exec)

	select {
	case msg := <-user.sentMu:
		got := msg.Payload.(*api.ExecResponse_Stdout).Stdout
		assert.Equal(t, payload, got)
	default:
		t.Fatal("no stdout forwarded to user")
	}
}

func TestBridge_ExecDoneForwarded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	user := newMockUserStream(ctx)
	exec := newMockExecStream(ctx)

	close(user.recvCh)
	exec.recvCh <- &proxyapi.ExecProxyMessage{
		Payload: &proxyapi.ExecProxyMessage_Done{Done: &proxyapi.ExecDone{ExitCode: 42}},
	}

	BridgeStreams(ctx, user, exec)

	select {
	case msg := <-user.sentMu:
		code := msg.Payload.(*api.ExecResponse_ExitCode).ExitCode
		assert.Equal(t, int32(42), code)
	default:
		t.Fatal("exit code not forwarded to user")
	}
}

func TestBridge_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	user := newMockUserStream(ctx)
	exec := newMockExecStream(ctx)

	done := make(chan struct{})
	go func() {
		BridgeStreams(ctx, user, exec)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("bridge did not exit after context cancel")
	}
}

func TestBridge_LargePayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	user := newMockUserStream(ctx)
	exec := newMockExecStream(ctx)

	const size = 1 << 20 // 1 MB
	payload := bytes.Repeat([]byte("z"), size)
	exec.recvCh <- &proxyapi.ExecProxyMessage{Payload: &proxyapi.ExecProxyMessage_Stdout{Stdout: payload}}
	exec.recvCh <- &proxyapi.ExecProxyMessage{Payload: &proxyapi.ExecProxyMessage_Done{Done: &proxyapi.ExecDone{}}}
	close(user.recvCh)

	BridgeStreams(ctx, user, exec)

	var received []byte
	for {
		select {
		case msg := <-user.sentMu:
			if p, ok := msg.Payload.(*api.ExecResponse_Stdout); ok {
				received = append(received, p.Stdout...)
			}
		default:
			require.Equal(t, size, len(received), "large payload not fully forwarded")
			return
		}
	}
}
