package ingest

import (
	"github.com/G-Research/armada/internal/common/util"
	log "github.com/sirupsen/logrus"
	"time"
)

func WithRetry(action func() (error, bool), maxBackoff int) error {
	backOff := 1
	for {
		err, retry := action()

		if err == nil {
			return nil
		}

		if retry {
			backOff = util.Min(2*backOff, maxBackoff)
			log.WithError(err).Warnf("Retryable error encountered inserting to Redis, will wait for %d seconds before retrying", backOff)
			time.Sleep(time.Duration(backOff) * time.Second)
		} else {
			// Non retryable error
			return err
		}
	}
}
