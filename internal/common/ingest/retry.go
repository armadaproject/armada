package ingest

import (
	"time"

	log "github.com/sirupsen/logrus"
)

// WithRetry executes the supplied action until it either completes successfully or it returns false, indicating that
// the error is fatal
func WithRetry(action func() (bool, error), intialBackoff time.Duration, maxBackOff time.Duration) error {
	backOff := intialBackoff
	for {
		retry, err := action()
		if err == nil {
			return nil
		}
		if retry {
			backOff = min(2*backOff, maxBackOff)
			log.WithError(err).Warnf("Retryable error encountered, will wait for %s before retrying", backOff)
			time.Sleep(backOff)
		} else {
			// Non retryable error
			return err
		}
	}
}

// min returns the minimum of two durations
func min(d1 time.Duration, d2 time.Duration) time.Duration {
	if d1.Nanoseconds() < d2.Nanoseconds() {
		return d1
	}
	return d2
}
