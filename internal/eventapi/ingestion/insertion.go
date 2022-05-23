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
func InsertEvents(ctx context.Context, db *eventdb.EventDb, msgs chan []*model.PulsarEventRow, bufferSize int) chan []*model.PulsarEventRow {
	out := make(chan []*model.PulsarEventRow, bufferSize)
	go func() {
		for msg := range msgs {
			insert(ctx, db, msg)
			out <- msg
		}
		close(out)
	}()
	return out
}

func insert(ctx context.Context, db *eventdb.EventDb, inputRows []*model.PulsarEventRow) {
	start := time.Now()
	rows := make([]*model.EventRow, len(inputRows))
	for i := 0; i < len(inputRows); i++ {
		rows[i] = inputRows[i].Event
	}
	err := db.UpdateEvents(ctx, rows)
	if err != nil {
		log.Warnf("Error inserting rows %+v", err)
	} else {
		taken := time.Now().Sub(start).Milliseconds()
		log.Infof("Inserted %d events in %dms", len(inputRows), taken)
	}
}
