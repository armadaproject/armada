package armadactl

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	"golang.org/x/term"
	"google.golang.org/grpc"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// ExecParams holds the parameters for the exec command.
type ExecParams struct {
	JobID     string
	RunID     string
	Container string
	PodNumber int32
	TTY       bool
	Stdin     bool
	Command   []string
}

// Exec opens a bidirectional exec stream to the Armada server and bridges it
// to the local terminal.
func (a *App) Exec(params ExecParams) error {
	return client.WithConnection(a.Params.ApiConnectionDetails, func(conn *grpc.ClientConn) error {
		return execWithConn(conn, params)
	})
}

func execWithConn(conn *grpc.ClientConn, params ExecParams) error {
	svcClient := api.NewInteractiveServiceClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := svcClient.Exec(ctx, grpc_retry.Disable())
	if err != nil {
		return fmt.Errorf("failed to open exec stream: %w", err)
	}

	// Send the init message.
	if err := stream.Send(&api.ExecRequest{
		Payload: &api.ExecRequest_Init{
			Init: &api.ExecInit{
				JobId:     params.JobID,
				RunId:     params.RunID,
				PodNumber: params.PodNumber,
				Container: params.Container,
				Command:   params.Command,
				Tty:       params.TTY,
				Stdin:     params.Stdin,
			},
		},
	}); err != nil {
		return fmt.Errorf("failed to send exec init: %w", err)
	}

	// Set up raw terminal if TTY was requested and stdin is a terminal.
	if params.TTY && term.IsTerminal(int(os.Stdin.Fd())) {
		oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err != nil {
			return fmt.Errorf("failed to set raw terminal mode: %w", err)
		}
		defer term.Restore(int(os.Stdin.Fd()), oldState)

		// Watch for terminal resize events.
		sigwinchCh := make(chan os.Signal, 1)
		signal.Notify(sigwinchCh, syscall.SIGWINCH)
		defer signal.Stop(sigwinchCh)
		go func() {
			for range sigwinchCh {
				w, h, err := term.GetSize(int(os.Stdin.Fd()))
				if err != nil {
					return
				}
				_ = stream.Send(&api.ExecRequest{
					Payload: &api.ExecRequest_Resize{
						Resize: &api.ExecResize{
							Width:  uint32(w),
							Height: uint32(h),
						},
					},
				})
			}
		}()
	}

	// stdin → stream
	if params.Stdin {
		go func() {
			buf := make([]byte, 1024)
			for {
				n, err := os.Stdin.Read(buf)
				if n > 0 {
					data := make([]byte, n)
					copy(data, buf[:n])
					if sendErr := stream.Send(&api.ExecRequest{
						Payload: &api.ExecRequest_Stdin{Stdin: data},
					}); sendErr != nil {
						return
					}
				}
				if err != nil {
					return
				}
			}
		}()
	}

	// stream → stdout/stderr
	exitCode := 0
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("stream error: %w", err)
		}
		switch p := resp.Payload.(type) {
		case *api.ExecResponse_Stdout:
			_, _ = os.Stdout.Write(p.Stdout)
		case *api.ExecResponse_Stderr:
			_, _ = os.Stderr.Write(p.Stderr)
		case *api.ExecResponse_ExitCode:
			exitCode = int(p.ExitCode)
		case *api.ExecResponse_Error:
			_, _ = fmt.Fprintln(os.Stderr, p.Error)
		}
	}

	if exitCode != 0 {
		return fmt.Errorf("exited with code %d", exitCode)
	}
	return nil
}
