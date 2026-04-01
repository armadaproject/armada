package proxy

import (
	"context"
	"io"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	"google.golang.org/grpc"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/pkg/proxyapi"
)

const (
	heartbeatInterval  = 30 * time.Second
	reconnectBaseDelay = 1 * time.Second
	reconnectMaxDelay  = 60 * time.Second
)

// ProxyControlClient maintains a persistent ProxyControl stream to the Armada
// server on behalf of this executor. It reconnects with exponential backoff.
type ProxyControlClient struct {
	executorID  string
	client      proxyapi.ExecutorProxyApiClient
	execHandler *ExecHandler
}

// NewProxyControlClient creates a ProxyControlClient.
func NewProxyControlClient(
	executorID string,
	conn *grpc.ClientConn,
	execHandler *ExecHandler,
) *ProxyControlClient {
	return &ProxyControlClient{
		executorID:  executorID,
		client:      proxyapi.NewExecutorProxyApiClient(conn),
		execHandler: execHandler,
	}
}

// Run connects and maintains the ProxyControl stream until ctx is cancelled.
// It reconnects with exponential backoff on any error.
func (p *ProxyControlClient) Run(ctx context.Context) error {
	delay := reconnectBaseDelay
	for {
		if err := p.runOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.WithError(err).Warnf("ProxyControl stream error; reconnecting in %s", delay)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		delay *= 2
		if delay > reconnectMaxDelay {
			delay = reconnectMaxDelay
		}
	}
}

func (p *ProxyControlClient) runOnce(ctx context.Context) error {
	stream, err := p.client.ProxyControl(ctx, grpc_retry.Disable())
	if err != nil {
		return err
	}

	// Send executor identity as the first message.
	if err := stream.Send(&proxyapi.ProxyControlMessage{
		Payload: &proxyapi.ProxyControlMessage_ExecutorId{ExecutorId: p.executorID},
	}); err != nil {
		return err
	}

	// Derive a per-connection context. When this connection drops, all
	// sessions started by this connection are cancelled.
	connCtx, connCancel := context.WithCancel(ctx)
	defer connCancel()

	// Heartbeat sender.
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-connCtx.Done():
				return
			case <-ticker.C:
				if err := stream.Send(&proxyapi.ProxyControlMessage{
					Payload: &proxyapi.ProxyControlMessage_Heartbeat{Heartbeat: &proxyapi.Heartbeat{}},
				}); err != nil {
					return
				}
			}
		}
	}()

	// Read server commands.
	for {
		cmd, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		switch payload := cmd.Command.(type) {
		case *proxyapi.ProxyControlCommand_StartExec:
			req := payload.StartExec
			sessionCtx, sessionCancel := context.WithCancel(connCtx)
			go func() {
				defer sessionCancel()
				if err := p.execHandler.Handle(sessionCtx, req); err != nil {
					log.WithError(err).Warnf("exec session %s error", req.SessionId)
				}
			}()
		}
	}
}
