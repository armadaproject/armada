package app

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

// CreateContextWithShutdown returns a context that will report done when a SIGTERM is received
func CreateContextWithShutdown() *armadacontext.Context {
	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx
}
