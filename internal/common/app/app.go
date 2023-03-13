package app

import (
	"context"
	"os"
	"os/signal"
)

// CreateContextWithShutdown returns a context that will report done when a SIGINT is received
func CreateContextWithShutdown() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx
}
