package ingest

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/util"
)

// WithRetry executes the supplied action until it either completes successfully or it returns false, indicating that
// the error is fatal
func WithRetry(action func() (error, bool), maxBackoff int) error {
	backOff := 1
	for {
		err, retry := action()

		if err == nil {
			return nil
		}

		if retry {
			backOff = util.Min(2*backOff, maxBackoff)
			log.WithError(err).Warnf("Retryable error encountered, will wait for %d seconds before retrying", backOff)
			time.Sleep(time.Duration(backOff) * time.Second)
		} else {
			// Non retryable error
			return err
		}
	}
}
