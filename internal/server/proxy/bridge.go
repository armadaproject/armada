package proxy

import (
	"context"
	"io"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/proxyapi"
)

// UserExecStream is the server-side half of the InteractiveService.Exec stream.
type UserExecStream interface {
	Send(*api.ExecResponse) error
	Recv() (*api.ExecRequest, error)
	Context() context.Context
}

// ExecutorExecStream is the server-side half of the ExecutorProxyApi.ExecProxy stream.
type ExecutorExecStream interface {
	Send(*proxyapi.ExecProxyMessage) error
	Recv() (*proxyapi.ExecProxyMessage, error)
	Context() context.Context
}

// BridgeStreams bridges a user-facing ExecRequest/ExecResponse stream with an
// executor-facing ExecProxyMessage stream.
//
// The sessionCtx should be derived from the executor's ProxyControl stream
// context — when the control stream drops, sessionCtx is cancelled and the
// bridge tears down automatically.
//
// BridgeStreams blocks until both directions have been drained and all
// goroutines have exited.
func BridgeStreams(sessionCtx context.Context, user UserExecStream, executor ExecutorExecStream) {
	bridgeCtx, bridgeCancel := context.WithCancel(sessionCtx)
	defer bridgeCancel()

	type result struct{ err error }
	userDone := make(chan result, 1)
	execDone := make(chan result, 1)

	// user -> executor: forward stdin and resize events.
	go func() {
		defer close(userDone)
		for {
			req, err := user.Recv()
			if err != nil {
				if err != io.EOF {
					userDone <- result{err}
				}
				return
			}
			var msg *proxyapi.ExecProxyMessage
			switch p := req.Payload.(type) {
			case *api.ExecRequest_Stdin:
				msg = &proxyapi.ExecProxyMessage{
					Payload: &proxyapi.ExecProxyMessage_Stdin{Stdin: p.Stdin},
				}
			case *api.ExecRequest_Resize:
				msg = &proxyapi.ExecProxyMessage{
					Payload: &proxyapi.ExecProxyMessage_Resize{
						Resize: &proxyapi.TerminalResize{
							Width:  p.Resize.Width,
							Height: p.Resize.Height,
						},
					},
				}
			default:
				continue
			}
			if err := executor.Send(msg); err != nil {
				userDone <- result{err}
				return
			}
		}
	}()

	// executor -> user: forward stdout, stderr, and done signals.
	go func() {
		defer close(execDone)
		for {
			msg, err := executor.Recv()
			if err != nil {
				if err != io.EOF {
					execDone <- result{err}
				}
				return
			}
			var resp *api.ExecResponse
			switch p := msg.Payload.(type) {
			case *proxyapi.ExecProxyMessage_Stdout:
				resp = &api.ExecResponse{
					Payload: &api.ExecResponse_Stdout{Stdout: p.Stdout},
				}
			case *proxyapi.ExecProxyMessage_Stderr:
				resp = &api.ExecResponse{
					Payload: &api.ExecResponse_Stderr{Stderr: p.Stderr},
				}
			case *proxyapi.ExecProxyMessage_Done:
				done := p.Done
				if done.Error != "" {
					_ = user.Send(&api.ExecResponse{
						Payload: &api.ExecResponse_Error{Error: done.Error},
					})
				} else {
					_ = user.Send(&api.ExecResponse{
						Payload: &api.ExecResponse_ExitCode{ExitCode: done.ExitCode},
					})
				}
				// Done means the session is over.
				bridgeCancel()
				return
			default:
				continue
			}
			if resp != nil {
				if err := user.Send(resp); err != nil {
					execDone <- result{err}
					return
				}
			}
		}
	}()

	// Supervisor: wait for the first side to finish.
	select {
	case <-execDone:
		// Executor sent Done — session is complete. Cancel and wait for the
		// user goroutine to flush any buffered stdin before returning.
		bridgeCancel()
		<-userDone
	case <-userDone:
		// User disconnected first. Wait for the executor goroutine to finish.
		bridgeCancel()
		<-execDone
	case <-bridgeCtx.Done():
		// Underlying context cancelled (e.g. executor control stream dropped).
		// Both goroutines will exit when the streams close.
	}
}
