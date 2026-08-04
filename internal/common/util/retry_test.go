package util

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

func TestRetryDoesntSpin(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 1*time.Second)
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
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 1*time.Second)
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

	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 1*time.Second)
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

func TestRetryUntilSuccessOrExhausted_SucceedsFirstTry(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 1*time.Second)
	defer cancel()

	errorCount := 0
	exhaustedCount := 0

	ok := RetryUntilSuccessOrExhausted(
		ctx,
		5,
		func() error {
			return nil
		},
		func(attempt int, err error) { errorCount++ },
		func(lastErr error) { exhaustedCount++ },
	)

	assert.True(t, ok)
	assert.Equal(t, 0, errorCount)
	assert.Equal(t, 0, exhaustedCount)
}

func TestRetryUntilSuccessOrExhausted_SucceedsWithinBudget(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 1*time.Second)
	defer cancel()

	ch := make(chan error, 3)
	ch <- fmt.Errorf("dummy error 1")
	ch <- fmt.Errorf("dummy error 2")
	ch <- nil

	errorCount := 0
	exhaustedCount := 0

	ok := RetryUntilSuccessOrExhausted(
		ctx,
		5,
		func() error {
			return <-ch
		},
		func(attempt int, err error) { errorCount++ },
		func(lastErr error) { exhaustedCount++ },
	)

	assert.True(t, ok)
	assert.Equal(t, 2, errorCount)
	assert.Equal(t, 0, exhaustedCount)
}

func TestRetryUntilSuccessOrExhausted_ExhaustsBudget(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 1*time.Second)
	defer cancel()

	dummyErr := fmt.Errorf("dummy error")
	errorCount := 0
	var exhaustedErr error

	ok := RetryUntilSuccessOrExhausted(
		ctx,
		5,
		func() error {
			return dummyErr
		},
		func(attempt int, err error) { errorCount++ },
		func(lastErr error) { exhaustedErr = lastErr },
	)

	assert.False(t, ok)
	assert.Equal(t, 5, errorCount)
	assert.Equal(t, dummyErr, exhaustedErr)

	select {
	case <-ctx.Done():
		t.Fatalf("Function exhausted budget but context was also cancelled unexpectedly.")
	default:
		break
	}
}

func TestRetryUntilSuccessOrExhausted_NonRetryableShortCircuits(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 1*time.Second)
	defer cancel()

	dummyErr := fmt.Errorf("dummy error")
	nonRetryableErr := fmt.Errorf("%w: %w", ErrNonRetryable, dummyErr)
	errorCount := 0
	var exhaustedErr error

	ok := RetryUntilSuccessOrExhausted(
		ctx,
		5,
		func() error {
			return nonRetryableErr
		},
		func(attempt int, err error) { errorCount++ },
		func(lastErr error) { exhaustedErr = lastErr },
	)

	assert.False(t, ok)
	assert.Equal(t, 0, errorCount)
	assert.Equal(t, nonRetryableErr, exhaustedErr)
}

func TestRetryUntilSuccessOrExhausted_NonRetryableAfterSomeRetries(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 1*time.Second)
	defer cancel()

	retryableErr := fmt.Errorf("retryable error")
	nonRetryableErr := fmt.Errorf("%w: %w", ErrNonRetryable, fmt.Errorf("non-retryable error"))

	ch := make(chan error, 3)
	ch <- retryableErr
	ch <- retryableErr
	ch <- nonRetryableErr

	errorCount := 0
	var exhaustedErr error

	ok := RetryUntilSuccessOrExhausted(
		ctx,
		5,
		func() error {
			return <-ch
		},
		func(attempt int, err error) { errorCount++ },
		func(lastErr error) { exhaustedErr = lastErr },
	)

	assert.False(t, ok)
	assert.Equal(t, 2, errorCount)
	assert.Equal(t, nonRetryableErr, exhaustedErr)
}

func TestIsNonRetryable(t *testing.T) {
	assert.False(t, errors.Is(nil, ErrNonRetryable))
	assert.False(t, errors.Is(fmt.Errorf("plain error"), ErrNonRetryable))
	assert.True(t, errors.Is(fmt.Errorf("%w: %w", ErrNonRetryable, fmt.Errorf("wrapped error")), ErrNonRetryable))
	assert.True(t, errors.Is(fmt.Errorf("outer: %w", fmt.Errorf("%w: %w", ErrNonRetryable, fmt.Errorf("inner"))), ErrNonRetryable))
}

func TestRetryUntilSuccessOrExhausted_NonPositiveMaxAttempts(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 1*time.Second)
	defer cancel()

	performed := false
	var exhaustedErr error

	ok := RetryUntilSuccessOrExhausted(
		ctx,
		0,
		func() error {
			performed = true
			return nil
		},
		func(attempt int, err error) {},
		func(lastErr error) { exhaustedErr = lastErr },
	)

	assert.False(t, ok)
	assert.False(t, performed)
	assert.Error(t, exhaustedErr)
}

func TestRetryUntilSuccessOrExhausted_CancelledMidRetry(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 50*time.Millisecond)
	defer cancel()

	dummyErr := fmt.Errorf("dummy error")
	exhaustedCount := 0

	ok := RetryUntilSuccessOrExhausted(
		ctx,
		1000000,
		func() error {
			time.Sleep(10 * time.Millisecond)
			return dummyErr
		},
		func(attempt int, err error) {},
		func(lastErr error) { exhaustedCount++ },
	)

	assert.False(t, ok)
	assert.Equal(t, 0, exhaustedCount)

	select {
	case <-ctx.Done():
		break
	default:
		t.Fatalf("Expected context to be done.")
	}
}
