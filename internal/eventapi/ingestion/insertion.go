package ingestion

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/eventapi/eventdb"
	"github.com/G-Research/armada/internal/eventapi/model"
)

// InsertEvents takes a channel of armada events and insets them into the event db
// the events are republished to an output channel for further processing (e.g. Ackking)
func InsertEvents(ctx context.Context, db *eventdb.EventDb, msgs chan *model.BatchUpdate, bufferSize int) chan *model.BatchUpdate {
	out := make(chan *model.BatchUpdate, bufferSize)
	go func() {
		for msg := range msgs {
			insert(ctx, db, msg.Events)
			out <- msg
		}
		close(out)
	}()
	return out
}

func insert(ctx context.Context, db *eventdb.EventDb, rows []*model.EventRow) {
	start := time.Now()
	err := db.UpdateEvents(ctx, rows)
	if err != nil {
		log.Warnf("Error inserting rows %+v", err)
	} else {
		taken := time.Now().Sub(start).Milliseconds()
		log.Infof("Inserted %d events in %dms", len(rows), taken)
	}
}
