package handler

import (
	"encoding/binary"
	"io"
	"net/http"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"

	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/pkg/api"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// ExecHandler is an HTTP handler that upgrades connections to WebSocket and
// bridges them to an InteractiveService.Exec gRPC stream on the Armada server.
type ExecHandler struct {
	grpcConn *grpc.ClientConn
}

// NewExecHandler creates an ExecHandler using an existing gRPC connection.
func NewExecHandler(grpcConn *grpc.ClientConn) *ExecHandler {
	return &ExecHandler{grpcConn: grpcConn}
}

func (h *ExecHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log := logging.StdLogger()

	jobId := r.URL.Query().Get("jobId")
	if jobId == "" {
		http.Error(w, "missing jobId query parameter", http.StatusBadRequest)
		return
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.WithError(err).Warn("WebSocket upgrade failed")
		return
	}

	if err := h.handleExec(r, ws, jobId); err != nil {
		log.WithError(err).Warn("exec session ended with error")
	}
}

func (h *ExecHandler) handleExec(r *http.Request, ws *websocket.Conn, jobId string) error {
	log := logging.StdLogger()
	defer ws.Close()

	client := api.NewInteractiveServiceClient(h.grpcConn)
	stream, err := client.Exec(r.Context())
	if err != nil {
		sendErrorFrame(ws, "failed to open exec stream: "+err.Error())
		return err
	}

	// Send ExecInit — server resolves executor/namespace from jobId
	initMsg := &api.ExecRequest{
		Payload: &api.ExecRequest_Init{
			Init: &api.ExecInit{
				JobId:   jobId,
				Command: []string{"/bin/sh"},
				Tty:     true,
				Stdin:   true,
			},
		},
	}
	if err := stream.Send(initMsg); err != nil {
		sendErrorFrame(ws, "failed to send ExecInit: "+err.Error())
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
