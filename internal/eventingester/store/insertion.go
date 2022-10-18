package store

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/eventingester/model"
	"github.com/G-Research/armada/internal/pulsarutils"
)

// InsertEvents takes a channel of armada events and inserts them into the event db
// the events are republished to an output channel for further processing (e.g. Ackking)
func InsertEvents(ctx context.Context, db EventStore, msgs chan *model.BatchUpdate, bufferSize int,
	maxSize int, maxRows int, fatalErrors []*regexp.Regexp,
) chan []*pulsarutils.ConsumerMessageId {
	out := make(chan []*pulsarutils.ConsumerMessageId, bufferSize)
	go func() {
		for msg := range msgs {
			insert(db, msg.Events, maxSize, maxRows, fatalErrors)
			out <- msg.MessageIds
		}
		close(out)
	}()
	return out
}

func insert(db EventStore, rows []*model.Event, maxSize int, maxRows int, fatalErrors []*regexp.Regexp) {
	if len(rows) == 0 {
		return
	}

	// Inset such that we never send more than maxRows rows or maxSize of data to redis at a time
	currentSize := 0
	currentRows := 0
	batch := make([]*model.Event, 0, maxRows)

	for i, event := range rows {
		newSize := currentSize + len(event.Event)
		newRows := currentRows + 1
		if newSize > maxSize || newRows > maxRows {
			doInsert(db, batch, fatalErrors)
			batch = make([]*model.Event, 0, maxRows)
			currentSize = 0
			currentRows = 0
		}
		batch = append(batch, event)
		currentSize += len(event.Event)
		currentRows++

		// If this is the last element we need to flush
		if i == len(rows)-1 {
			doInsert(db, batch, fatalErrors)
		}
	}
}

func doInsert(db EventStore, rows []*model.Event, fatalErrors []*regexp.Regexp) {
	start := time.Now()
	err := WithRetry(func() error {
		return db.ReportEvents(rows)
	}, fatalErrors)
	if err != nil {
		log.WithError(err).Warnf("Error inserting rows")
	} else {
		taken := time.Now().Sub(start).Milliseconds()
		log.Infof("Inserted %d events in %dms", len(rows), taken)
	}
}

func WithRetry(executeDb func() error, nonRetryableErrors []*regexp.Regexp) error {
	// TODO: arguably this should come from config
	backOff := 1
	const maxBackoff = 60
	const maxRetries = 10
	numRetries := 0
	var err error = nil
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := executeDb()

		if err == nil {
			return nil
		}

		if armadaerrors.IsNetworkError(err) || IsRetryableRedisError(err, nonRetryableErrors) {
			backOff = util.Min(2*backOff, maxBackoff)
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

// IsRetryableRedisError returns true if the error doesn't match the list of nonRetryableErrors
func IsRetryableRedisError(err error, nonRetryableErrors []*regexp.Regexp) bool {
	if err == nil {
		return true
	}
	s := err.Error()
	for _, r := range nonRetryableErrors {
		if r.MatchString(s) {
			log.Infof("Error %s matched regex %s and so will be considered fatal", s, r)
			return false
		}
	}
	return true
}
