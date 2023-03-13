package app

import (
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCreateContextWithShutdownSIGINTSignal(t *testing.T) {
	ctx := CreateContextWithShutdown()
	if ctx == nil {
		t.Errorf("CreateContextWithShutdown returned a nil context")
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Errorf("CreateContextWithShutdown did not shut down within 5 seconds")
		}
	}()

	err := syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	require.NoError(t, err)
	wg.Wait()
}

func TestCreateContextWithShutdownNoSignal(t *testing.T) {
	ctx := CreateContextWithShutdown()
	require.NotNil(t, ctx)

	select {
	case <-ctx.Done():
		t.Errorf("CreateContextWithShutdown shut down without SIGINT signal")
	case <-time.After(5 * time.Second):
	}
}
