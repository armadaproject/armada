package ingestion

import (
	"context"
	"github.com/G-Research/armada/internal/eventapi/model"
)

func ProcessUpdates(ctx context.Context, db *eventdb.EventDb, msgs chan []*model.PulsarEventRow, bufferSize int) chan []*model.PulsarEventRow {
	out := make(chan []*model.PulsarEventRow, bufferSize)
	go func() {
		for msg := range msgs {
			update(ctx, db, msg)
			out <- msg
		}
		close(out)
	}()
	return out
}

func update(ctx context.Context, db *eventdb.EventDb, inputRows []*model.PulsarEventRow) {
	rows := make([]*model.EventRow, len(inputRows))
	for i := 0; i <= len(inputRows); i++ {
		rows[i] = inputRows[i].Event
	}
	err := db.InsertEvents(ctx, rows)
	if err != nil {
		log.Warnf("Error inserting rows")
	}
}
