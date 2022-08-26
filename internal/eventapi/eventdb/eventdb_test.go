package eventdb

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/common/database"
	"github.com/G-Research/armada/internal/eventapi/eventdb/schema/statik"
	"github.com/G-Research/armada/internal/eventapi/model"
)

var baseTime, _ = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")

func TestInsertSequenceNumbers(t *testing.T) {
	err := WithDatabase(func(db *EventDb) error {
		ctx := context.Background()
		seqNos := []*model.SeqNoRow{
			{
				JobSetId:   1,
				SeqNo:      1,
				UpdateTime: baseTime,
			},
			{
				JobSetId:   2,
				SeqNo:      1,
				UpdateTime: baseTime,
			},
		}
		err := db.InsertSeqNos(ctx, seqNos)
		assert.NoError(t, err)
		retrievedSeqNos, err := db.LoadSeqNos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, seqNos, retrievedSeqNos)

		// Test idempotant by inserting again
		err = db.InsertSeqNos(ctx, seqNos)
		assert.NoError(t, err)
		retrievedSeqNos, err = db.LoadSeqNos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, seqNos, retrievedSeqNos)

		return nil
	})
	assert.NoError(t, err)
}

func TestUpdateEventsWithoutDuplicates(t *testing.T) {
	err := WithDatabase(func(db *EventDb) error {
		ctx := context.Background()
		events := []*model.EventRow{
			{
				JobSetId: 1,
				SeqNo:    1,
				Event:    []byte{1},
			},
			{
				JobSetId: 1,
				SeqNo:    2,
				Event:    []byte{2},
			},
			{
				JobSetId: 2,
				SeqNo:    3,
				Event:    []byte{3},
			},
		}
		err := db.UpdateEvents(ctx, events)
		assert.NoError(t, err)
		retrievedEvents, err := db.LoadEvents(ctx)
		assert.NoError(t, err)
		assert.Equal(t, events, retrievedEvents)

		// Check seq nos were updated
		expectedSeqNos := []*model.SeqNoRow{
			{JobSetId: 1, SeqNo: 2},
			{JobSetId: 2, SeqNo: 3},
		}
		retrievedSeqNos, err := db.LoadSeqNos(ctx)
		assert.NoError(t, err)
		for i, sn := range retrievedSeqNos {
			expectedSeqNos[i].UpdateTime = sn.UpdateTime
		}
		assert.Equal(t, expectedSeqNos, retrievedSeqNos)
		return nil
	})
	assert.NoError(t, err)
}

func TestUpdateEventsWithDuplicates(t *testing.T) {
	err := WithDatabase(func(db *EventDb) error {
		ctx := context.Background()
		intialSeqNos := []*model.SeqNoRow{
			{
				JobSetId:   1,
				SeqNo:      1,
				UpdateTime: baseTime,
			},
			{
				JobSetId:   2,
				SeqNo:      3,
				UpdateTime: baseTime,
			},
		}

		err := db.InsertSeqNos(ctx, intialSeqNos)
		assert.NoError(t, err)

		events := []*model.EventRow{
			{
				JobSetId: 1,
				SeqNo:    1,
				Event:    []byte{1},
			},
			{
				JobSetId: 1,
				SeqNo:    2,
				Event:    []byte{2},
			},
			{
				JobSetId: 2,
				SeqNo:    3,
				Event:    []byte{3},
			},
		}
		err = db.UpdateEvents(ctx, events)
		assert.NoError(t, err)

		expectedEvents := []*model.EventRow{
			{
				JobSetId: 1,
				SeqNo:    2,
				Event:    []byte{2},
			},
		}

		retrievedEvents, err := db.LoadEvents(ctx)
		assert.NoError(t, err)
		assert.Equal(t, expectedEvents, retrievedEvents)

		// Check seq nos were updated
		expectedSeqNos := []*model.SeqNoRow{
			{JobSetId: 1, SeqNo: 2},
			{JobSetId: 2, SeqNo: 3},
		}
		retrievedSeqNos, err := db.LoadSeqNos(ctx)
		assert.NoError(t, err)
		for i, sn := range retrievedSeqNos {
			expectedSeqNos[i].UpdateTime = sn.UpdateTime
		}
		assert.Equal(t, expectedSeqNos, retrievedSeqNos)
		return nil
	})
	assert.NoError(t, err)
}

func TestGetOrCreateJobsetId(t *testing.T) {
	err := WithDatabase(func(db *EventDb) error {
		ctx := context.Background()
		id1, err := db.GetOrCreateJobsetId(ctx, "fish", "chips")
		assert.NoError(t, err)
		id2, err := db.GetOrCreateJobsetId(ctx, "fish", "vinegar")
		assert.NoError(t, err)
		id3, err := db.GetOrCreateJobsetId(ctx, "fish", "chips")
		assert.NoError(t, err)
		id4, err := db.GetOrCreateJobsetId(ctx, "fish", "vinegar")
		assert.NoError(t, err)

		assert.NotEqual(t, id1, id2)
		assert.Equal(t, id1, id3)
		assert.Equal(t, id2, id4)

		createdJobsets, err := db.LoadJobsetsAfter(ctx, time.Now().In(time.UTC).Add(-1*time.Minute))
		assert.NoError(t, err)
		expectedJobsets := []*model.JobsetRow{
			{JobSetId: 1, Queue: "fish", Jobset: "chips"},
			{JobSetId: 2, Queue: "fish", Jobset: "vinegar"},
		}
		for i, js := range createdJobsets {
			expectedJobsets[i].Created = js.Created
		}
		assert.Equal(t, expectedJobsets, createdJobsets)
		return nil
	})
	assert.NoError(t, err)
}

func TestGetEvents(t *testing.T) {
	err := WithDatabase(func(db *EventDb) error {
		ctx := context.Background()

		events := []*model.EventRow{
			{
				JobSetId: 1,
				SeqNo:    1,
				Event:    []byte{1},
			},
			{
				JobSetId: 1,
				SeqNo:    2,
				Event:    []byte{2},
			},
			{
				JobSetId: 2,
				SeqNo:    1,
				Event:    []byte{3},
			},
		}
		err := db.InsertEvents(ctx, events)
		assert.NoError(t, err)

		// Simple fetch of jobset1 events
		expectedEventsResponse := []*model.EventResponse{{
			SubscriptionId: 1,
			Events: []*model.EventRow{
				{JobSetId: 1, SeqNo: 1, Event: []byte{1}},
				{JobSetId: 1, SeqNo: 2, Event: []byte{2}},
			},
		}}
		assert.NoError(t, err)
		request := &model.EventRequest{SubscriptionId: 1, Jobset: 1, Sequence: -1}
		response, err := db.GetEvents([]*model.EventRequest{request}, 100)
		assert.NoError(t, err)
		assert.Equal(t, expectedEventsResponse, response)

		// Fetch jobset1 events after seqNo = 1
		expectedEventsResponse = []*model.EventResponse{{
			SubscriptionId: 1,
			Events: []*model.EventRow{
				{JobSetId: 1, SeqNo: 2, Event: []byte{2}},
			},
		}}
		assert.NoError(t, err)
		request = &model.EventRequest{SubscriptionId: 1, Jobset: 1, Sequence: 1}
		response, err = db.GetEvents([]*model.EventRequest{request}, 100)
		assert.NoError(t, err)
		assert.Equal(t, expectedEventsResponse, response)

		// Check that limits work
		expectedEventsResponse = []*model.EventResponse{{
			SubscriptionId: 1,
			Events: []*model.EventRow{
				{JobSetId: 1, SeqNo: 1, Event: []byte{1}},
			},
		}}
		assert.NoError(t, err)
		request = &model.EventRequest{SubscriptionId: 1, Jobset: 1, Sequence: -1}
		response, err = db.GetEvents([]*model.EventRequest{request}, 1)
		assert.NoError(t, err)
		assert.Equal(t, expectedEventsResponse, response)

		// Fetch multiple subscriptions at once
		expectedEventsResponse = []*model.EventResponse{
			{
				SubscriptionId: 1,
				Events: []*model.EventRow{
					{JobSetId: 1, SeqNo: 1, Event: []byte{1}},
					{JobSetId: 1, SeqNo: 2, Event: []byte{2}},
				},
			},
			{
				SubscriptionId: 2,
				Events: []*model.EventRow{
					{JobSetId: 2, SeqNo: 1, Event: []byte{3}},
				},
			},
		}
		assert.NoError(t, err)
		request = &model.EventRequest{SubscriptionId: 1, Jobset: 1, Sequence: -1}
		request2 := &model.EventRequest{SubscriptionId: 2, Jobset: 2, Sequence: -1}
		response, err = db.GetEvents([]*model.EventRequest{request, request2}, 100)
		assert.NoError(t, err)
		assert.Equal(t, expectedEventsResponse, response)
		return nil
	})
	assert.NoError(t, err)
}

func WithDatabase(action func(db *EventDb) error) error {
	migrations, err := database.GetMigrations(statik.EventapiSql)
	if err != nil {
		return err
	}
	return database.WithTestDb(migrations, func(db *pgxpool.Pool) error {
		return action(NewEventDb(db))
	})
}
