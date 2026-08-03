package util

import (
	"errors"
	"fmt"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

// NonRetryableError wraps an error to signal that it should not be retried, even if
// remaining attempts are available. Sinks can wrap errors with this to short-circuit
// RetryUntilSuccessOrExhausted straight to onExhausted.
type NonRetryableError struct {
	err error
}

func NewNonRetryableError(err error) *NonRetryableError {
	if err == nil {
		err = errors.New("non-retryable error")
	}
	return &NonRetryableError{err: err}
}

func (e *NonRetryableError) Error() string {
	return e.err.Error()
}

func (e *NonRetryableError) Unwrap() error {
	return e.err
}

func IsNonRetryable(err error) bool {
	var nonRetryable *NonRetryableError
	return errors.As(err, &nonRetryable)
}

func RetryUntilSuccess(ctx *armadacontext.Context, performAction func() error, onError func(error)) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := performAction()
			if err == nil {
				return
			} else {
				onError(err)
			}
		}
	}
}

// RetryUntilSuccessOrExhausted behaves like RetryUntilSuccess but gives up after
// maxAttempts consecutive failures, calling onExhausted with the last error
// instead of continuing. Returns true on eventual success, false otherwise.
// onExhausted is NOT called if ctx is cancelled first (shutdown case) - callers
// must distinguish "gave up due to shutdown" from "exhausted attempts" via ctx.Err().
// If performAction returns an error wrapped with NewNonRetryableError, remaining
// attempts are skipped and onExhausted is called immediately with that error.
// If maxAttempts is non-positive, performAction is never called and onExhausted
// is called with a placeholder error describing this.
func RetryUntilSuccessOrExhausted(
	ctx *armadacontext.Context,
	maxAttempts int,
	performAction func() error,
	onError func(attempt int, err error),
	onExhausted func(lastErr error),
) bool {
	lastErr := fmt.Errorf("no attempts were made: maxAttempts was %d", maxAttempts)
attempts:
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return false
		default:
			err := performAction()
			if err == nil {
				return true
			}
			lastErr = err
			if IsNonRetryable(err) {
				break attempts
			}
			onError(attempt, err)
		}
	}
	select {
	case <-ctx.Done():
		return false
	default:
		onExhausted(lastErr)
		return false
	}
}
