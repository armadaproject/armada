package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	k8sexec "k8s.io/client-go/util/exec"

	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/pkg/proxyapi"
)

// ExecHandler handles StartExecSession commands from the server by finding the
// target pod, opening a gRPC ExecProxy stream, and bridging to the K8s SPDY
// exec connection.
type ExecHandler struct {
	k8sClient       kubernetes.Interface
	restConfig      *rest.Config
	proxyClient     proxyapi.ExecutorProxyApiClient
	executorFactory SPDYExecutorFactory
}

// NewExecHandler creates an ExecHandler.
func NewExecHandler(
	k8sClient kubernetes.Interface,
	restConfig *rest.Config,
	proxyClient proxyapi.ExecutorProxyApiClient,
	executorFactory SPDYExecutorFactory,
) *ExecHandler {
	return &ExecHandler{
		k8sClient:       k8sClient,
		restConfig:      restConfig,
		proxyClient:     proxyClient,
		executorFactory: executorFactory,
	}
}

// Handle executes one exec session. Called in a goroutine per session.
func (h *ExecHandler) Handle(ctx context.Context, req *proxyapi.StartExecSession) error {
	// 1. Find the target pod using Armada label selectors.
	selector := fmt.Sprintf("%s=%s,%s=%d",
		domain.JobId, req.JobId,
		domain.PodNumber, req.PodNumber,
	)
	pods, err := h.k8sClient.CoreV1().Pods(req.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		return h.sendError(ctx, req.SessionId, fmt.Sprintf("failed to list pods: %v", err))
	}
	if len(pods.Items) == 0 {
		return h.sendError(ctx, req.SessionId, "pod not found")
	}
	pod := pods.Items[0]

	// 2. Open an ExecProxy stream to the server and send session_id.
	stream, err := h.proxyClient.ExecProxy(ctx, grpc_retry.Disable())
	if err != nil {
		return fmt.Errorf("failed to open ExecProxy stream: %w", err)
	}
	if err := stream.Send(&proxyapi.ExecProxyMessage{
		Payload: &proxyapi.ExecProxyMessage_SessionId{SessionId: req.SessionId},
	}); err != nil {
		return fmt.Errorf("failed to send session_id: %w", err)
	}

	// 3. Determine the target container.
	container := req.Container
	if container == "" && len(pod.Spec.Containers) > 0 {
		container = pod.Spec.Containers[0].Name
	}

	// 4. Build the K8s exec URL.
	execReq := h.k8sClient.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(req.Namespace).
		SubResource("exec")
	execReq.VersionedParams(&corev1.PodExecOptions{
		Container: container,
		Command:   req.Command,
		Stdin:     req.Stdin,
		Stdout:    true,
		Stderr:    true,
		TTY:       req.Tty,
	}, scheme.ParameterCodec)

	// 5. Create the SPDY executor.
	executor, err := h.executorFactory(h.restConfig, http.MethodPost, execReq.URL())
	if err != nil {
		return h.sendError(ctx, req.SessionId, fmt.Sprintf("failed to create SPDY executor: %v", err))
	}

	// 6. Set up stdin pipe.
	stdinReader, stdinWriter := io.Pipe()
	defer stdinWriter.Close()

	// Forward incoming gRPC messages to stdin (in a separate goroutine).
	go func() {
		defer stdinWriter.Close()
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			switch p := msg.Payload.(type) {
			case *proxyapi.ExecProxyMessage_Stdin:
				if _, err := stdinWriter.Write(p.Stdin); err != nil {
					return
				}
			}
		}
	}()

	// 7. Stream — blocks until the remote command exits.
	stdoutWriter := &grpcWriter{stream: stream, payloadType: "stdout"}
	stderrWriter := &grpcWriter{stream: stream, payloadType: "stderr"}

	streamErr := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  stdinReader,
		Stdout: stdoutWriter,
		Stderr: stderrWriter,
		Tty:    req.Tty,
	})

	// 8. Send ExecDone with exit code or error.
	exitCode := int32(0)
	errStr := ""
	if streamErr != nil {
		var codeExitErr k8sexec.CodeExitError
		if errors.As(streamErr, &codeExitErr) {
			exitCode = int32(codeExitErr.Code)
		} else {
			errStr = streamErr.Error()
		}
	}

	_ = stream.Send(&proxyapi.ExecProxyMessage{
		Payload: &proxyapi.ExecProxyMessage_Done{
			Done: &proxyapi.ExecDone{
				ExitCode: exitCode,
				Error:    errStr,
			},
		},
	})
	return nil
}

func (h *ExecHandler) sendError(ctx context.Context, sessionID, msg string) error {
	stream, err := h.proxyClient.ExecProxy(ctx, grpc_retry.Disable())
	if err != nil {
		return fmt.Errorf("sendError (could not open stream): %w", err)
	}
	_ = stream.Send(&proxyapi.ExecProxyMessage{
		Payload: &proxyapi.ExecProxyMessage_SessionId{SessionId: sessionID},
	})
	_ = stream.Send(&proxyapi.ExecProxyMessage{
		Payload: &proxyapi.ExecProxyMessage_Done{
			Done: &proxyapi.ExecDone{Error: msg},
		},
	})
	return fmt.Errorf("%s", msg)
}

// grpcWriter adapts gRPC ExecProxy stream sends to io.Writer.
type grpcWriter struct {
	stream      proxyapi.ExecutorProxyApi_ExecProxyClient
	payloadType string
}

func (w *grpcWriter) Write(p []byte) (int, error) {
	buf := make([]byte, len(p))
	copy(buf, p)
	var msg *proxyapi.ExecProxyMessage
	switch w.payloadType {
	case "stdout":
		msg = &proxyapi.ExecProxyMessage{
			Payload: &proxyapi.ExecProxyMessage_Stdout{Stdout: buf},
		}
	case "stderr":
		msg = &proxyapi.ExecProxyMessage{
			Payload: &proxyapi.ExecProxyMessage_Stderr{Stderr: buf},
		}
	default:
		return len(p), nil
	}
	if err := w.stream.Send(msg); err != nil {
		return 0, err
	}
	return len(p), nil
}
