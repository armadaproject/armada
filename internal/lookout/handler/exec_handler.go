package handler

import (
	"encoding/binary"
	"io"
	"net/http"
	"regexp"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/pkg/api"
)

// containerNameRE matches valid Kubernetes container names.
var containerNameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// ExecHandler is an HTTP handler that upgrades connections to WebSocket and
// bridges them to an InteractiveService.Exec gRPC stream on the Armada server.
type ExecHandler struct {
	grpcConn      *grpc.ClientConn
	allowedOrigins []string
}

// NewExecHandler creates an ExecHandler using an existing gRPC connection.
// allowedOrigins is the list of CORS-allowed origins; an empty list means
// same-origin requests only (Origin header must match the Host header).
func NewExecHandler(grpcConn *grpc.ClientConn, allowedOrigins []string) *ExecHandler {
	return &ExecHandler{grpcConn: grpcConn, allowedOrigins: allowedOrigins}
}

func (h *ExecHandler) upgrader() websocket.Upgrader {
	allowed := h.allowedOrigins
	return websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			if origin == "" {
				// No Origin header — same-origin request from non-browser client; allow.
				return true
			}
			// If an explicit allow-list is configured, enforce it.
			if len(allowed) > 0 {
				for _, o := range allowed {
					if o == origin {
						return true
					}
				}
				return false
			}
			// No explicit list: fall back to same-origin (Origin matches Host).
			return origin == "http://"+r.Host || origin == "https://"+r.Host
		},
	}
}

func (h *ExecHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log := logging.StdLogger()

	jobId := r.URL.Query().Get("jobId")
	if jobId == "" {
		http.Error(w, "missing jobId query parameter", http.StatusBadRequest)
		return
	}

	container := r.URL.Query().Get("container")
	if container != "" && !containerNameRE.MatchString(container) {
		http.Error(w, "invalid container name", http.StatusBadRequest)
		return
	}

	upgrader := h.upgrader()
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.WithError(err).Warn("WebSocket upgrade failed")
		return
	}

	if err := h.handleExec(r, ws, jobId, container); err != nil {
		log.WithError(err).Warn("exec session ended with error")
	}
}

func (h *ExecHandler) handleExec(r *http.Request, ws *websocket.Conn, jobId, container string) error {
	log := logging.StdLogger()
	defer ws.Close()

	client := api.NewInteractiveServiceClient(h.grpcConn)

	// Forward the user's Bearer token to the Armada gRPC server. The token
	// arrives as an Authorization header (synthesized from ?token= for WebSocket
	// upgrades in configure_lookout.go). gRPC interceptors read auth from
	// outgoing metadata, not from the Go context value set by the HTTP middleware.
	ctx := r.Context()
	if authHeader := r.Header.Get("Authorization"); authHeader != "" {
		md := metadata.Pairs("authorization", authHeader)
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	stream, err := client.Exec(ctx)
	if err != nil {
		log.WithError(err).Warn("failed to open exec stream")
		sendErrorFrame(ws, "failed to connect to exec service")
		return err
	}

	// Send ExecInit — server resolves executor/namespace from jobId
	initMsg := &api.ExecRequest{
		Payload: &api.ExecRequest_Init{
			Init: &api.ExecInit{
				JobId:     jobId,
				Container: container,
				Command:   []string{"/bin/sh"},
				Tty:       true,
				Stdin:     true,
			},
		},
	}
	if err := stream.Send(initMsg); err != nil {
		log.WithError(err).Warn("failed to send ExecInit")
		sendErrorFrame(ws, "failed to start exec session")
		return err
	}

	// ws→gRPC goroutine result channel
	wsToGrpcDone := make(chan error, 1)
	go func() {
		wsToGrpcDone <- forwardWSToGRPC(ws, stream)
	}()

	// gRPC→WS goroutine result channel
	grpcToWsDone := make(chan error, 1)
	go func() {
		grpcToWsDone <- forwardGRPCToWS(stream, ws)
	}()

	// Wait for either direction to finish
	select {
	case err = <-wsToGrpcDone:
		if err != nil && err != io.EOF {
			log.WithError(err).Debug("ws→grpc direction finished")
		}
		stream.CloseSend()
		<-grpcToWsDone
	case err = <-grpcToWsDone:
		if err != nil && err != io.EOF {
			log.WithError(err).Debug("grpc→ws direction finished")
		}
		<-wsToGrpcDone
	}
	return nil
}

// forwardWSToGRPC reads binary frames from the WebSocket and forwards them to the gRPC stream.
//
// Binary protocol (browser → server):
//
//	0x00 + stdin bytes
//	0x01 + uint16 BE cols + uint16 BE rows
func forwardWSToGRPC(ws *websocket.Conn, stream api.InteractiveService_ExecClient) error {
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			return err
		}
		if len(data) == 0 {
			continue
		}
		msgType := data[0]
		payload := data[1:]

		switch msgType {
		case 0x00:
			// stdin
			msg := &api.ExecRequest{
				Payload: &api.ExecRequest_Stdin{
					Stdin: payload,
				},
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		case 0x01:
			// resize: 2-byte cols + 2-byte rows
			if len(payload) < 4 {
				continue
			}
			cols := binary.BigEndian.Uint16(payload[0:2])
			rows := binary.BigEndian.Uint16(payload[2:4])
			msg := &api.ExecRequest{
				Payload: &api.ExecRequest_Resize{
					Resize: &api.ExecResize{
						Width:  uint32(cols),
						Height: uint32(rows),
					},
				},
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}

// forwardGRPCToWS reads ExecResponse messages from the gRPC stream and forwards them to the WebSocket.
//
// Binary protocol (server → browser):
//
//	0x00 + output bytes (stdout/stderr merged)
//	0x01 + int32 BE exit code
//	0x02 + UTF-8 error message
func forwardGRPCToWS(stream api.InteractiveService_ExecClient, ws *websocket.Conn) error {
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		switch p := resp.Payload.(type) {
		case *api.ExecResponse_Stdout:
			if err := ws.WriteMessage(websocket.BinaryMessage, append([]byte{0x00}, p.Stdout...)); err != nil {
				return err
			}
		case *api.ExecResponse_Stderr:
			// Merge stderr with stdout for the browser (same channel 0x00)
			if err := ws.WriteMessage(websocket.BinaryMessage, append([]byte{0x00}, p.Stderr...)); err != nil {
				return err
			}
		case *api.ExecResponse_ExitCode:
			buf := make([]byte, 5)
			buf[0] = 0x01
			binary.BigEndian.PutUint32(buf[1:], uint32(p.ExitCode))
			if err := ws.WriteMessage(websocket.BinaryMessage, buf); err != nil {
				return err
			}
			return nil
		case *api.ExecResponse_Error:
			if err := sendErrorFrame(ws, p.Error); err != nil {
				return err
			}
			return nil
		}
	}
}

func sendErrorFrame(ws *websocket.Conn, msg string) error {
	return ws.WriteMessage(websocket.BinaryMessage, append([]byte{0x02}, []byte(msg)...))
}
