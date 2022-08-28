package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/eventingester/model"
	"github.com/G-Research/armada/internal/pulsarutils"
)

// InsertEvents takes a channel of armada events and insets them into the event db
// the events are republished to an output channel for further processing (e.g. Ackking)
func InsertEvents(ctx context.Context, db *RedisEventStore, msgs chan *model.BatchUpdate, bufferSize int) chan []*pulsarutils.ConsumerMessageId {
	out := make(chan []*pulsarutils.ConsumerMessageId, bufferSize)
	go func() {
		for msg := range msgs {
			insert(db, msg.Events)
			out <- msg.MessageIds
		}
		close(out)
	}()
	return out
}

func insert(db *RedisEventStore, rows []*model.Event) {
	start := time.Now()

	err := WithRetry(func() error {
		return db.ReportEvents(rows)
	})

	if err != nil {
		log.WithError(err).Warnf("Error inserting rows")
	} else {
		taken := time.Now().Sub(start).Milliseconds()
		log.Infof("Inserted %d events in %dms", len(rows), taken)
	}
}

func WithRetry(executeDb func() error) error {
	// TODO: arguably this should come from config
	var backOff = 1
	const maxBackoff = 60
	const maxRetries = 10
	var numRetries = 0
	var err error = nil
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := executeDb()

		if err == nil {
			return nil
		}

		if armadaerrors.IsNetworkError(err) || IsRetryableRedisError(err) {
			backOff = min(2*backOff, maxBackoff)
			numRetries++
			log.WithError(err).Warnf("Retryable error encountered inserting to Redis, will wait for %d seconds before retrying", backOff)
			time.Sleep(time.Duration(backOff) * time.Second)
		} else {
			// Non retryable error
			return err
		}
	}

	// If we get to here then we've got an error we can't handle. Panic
	panic(errors.WithStack(&armadaerrors.ErrMaxRetriesExceeded{
		Message:   fmt.Sprintf("Gave up inserting into Redis after %d retries", maxRetries),
		LastError: err,
	}))
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// IsRetryableRedisError is largely taken from https://github.com/go-redis/redis/blob/master/error.go#L28
func IsRetryableRedisError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	if s == "ERR max number of clients reached" {
		return true
	}
	if strings.HasPrefix(s, "LOADING ") {
		return true
	}
	if strings.HasPrefix(s, "READONLY ") {
		return true
	}
	if strings.HasPrefix(s, "CLUSTERDOWN ") {
		return true
	}
	if strings.HasPrefix(s, "TRYAGAIN ") {
		return true
	}
	return false
}
