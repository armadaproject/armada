package util

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/context"
)

func TestRetryDoesntSpin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	RetryUntilSuccess(
		ctx,
		func() error {
			return nil
		},
		func(err error) {},
	)

	select {
	case <-ctx.Done():
		t.Fatalf("Function did not complete within time limit.")
	default:
		break
	}
}

func TestRetryCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	RetryUntilSuccess(
		ctx,
		func() error {
			return fmt.Errorf("Dummy error.")
		},
		func(err error) {},
	)

	select {
	case <-ctx.Done():
		break
	default:
		t.Fatalf("Function exit was early.")
	}
}

func TestSucceedsAfterFailures(t *testing.T) {
	ch := make(chan error, 6)
	err := fmt.Errorf("Dummy error.")

	// Load up the channel with my errors
	for range [5]int{} {
		ch <- err
	}
	ch <- nil

	errorCount := 0

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	RetryUntilSuccess(
		ctx,
		func() error {
			return <-ch
		},
		func(err error) {
			errorCount += 1
		},
	)

	select {
	case <-ctx.Done():
		t.Fatalf("Function timed out.")
	default:
		break
	}

	assert.Equal(t, 5, errorCount)
}
