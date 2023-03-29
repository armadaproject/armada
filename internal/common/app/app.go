package app

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// CreateContextWithShutdown returns a context that will report done when a SIGTERM is received
func CreateContextWithShutdown() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
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
