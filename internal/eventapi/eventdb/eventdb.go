package eventdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"

	"github.com/G-Research/armada/internal/common/database"
	"github.com/G-Research/armada/internal/eventapi/model"
)

// EventDb represents the operations that can be performed on the events database
type EventDb struct {
	db *pgxpool.Pool
}

func NewEventDb(db *pgxpool.Pool) *EventDb {
	return &EventDb{db: db}
}

// UpdateEvents updates the database with a batch of events.
// This first check the database to see which sequence numbers have already been inserted, any Events with lower
// sequence numbers are discarded at this stage.
// Next the events are insrted using the postgres copy protocol
// Finally the sequence numbers are updated
func (e *EventDb) UpdateEvents(ctx context.Context, events []*model.EventRow) error {
	jobsetIds := distinctJobsets(events)

	// If there's nothing to insert then we can just return
	if len(jobsetIds) < 1 {
		log.Debugf("No events to insert")
		return nil
	}

	return e.db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		// First filter out any events where there is no row or the sequenceNo is equal to or lower than sequenceNos we already have
		sequenceNos, err := loadSequenceNosForIds(ctx, tx, jobsetIds)
		if err != nil {
			return err
		}
		eventsToInsert := make([]*model.EventRow, 0)
		for _, event := range events {
			if event != nil {
				lastProcessed, seqNoFound := sequenceNos[event.JobSetId]
				if !seqNoFound || lastProcessed < event.SeqNo {
					eventsToInsert = append(eventsToInsert, event)
				}
			}
		}

		// Now insert the Events
		err = e.InsertEvents(ctx, eventsToInsert)
		if err != nil {
			return err
		}

		// Finally, insert the seqnos corresponding to those events
		seqNosToInsert := lastSeqNumbers(eventsToInsert)
		err = e.InsertSeqNos(ctx, seqNosToInsert)
		return err
	})
}

// InsertSeqNos inserts a batch of sequence numbers using the postgres copy protocol
// This is effectively an upsert.  If a sequence number for a jobset didn't already exist then it is created.
// If it does already exist then the corresponding sequence number is updated
// Note that it is up to the caller to determine that they are not replacing more recent sequence numbers
func (e *EventDb) InsertSeqNos(ctx context.Context, seqNos []*model.SeqNoRow) error {
	tmpTable := database.UniqueTableName("latest_seqno")

	createTmp := func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s 
				(
				  jobset_id 	bigint,
				  seqno         bigint,
				  update_time   timestamp
				) ON COMMIT DROP;`, tmpTable))
		return errors.WithStack(err)
	}

	insertTmp := func(tx pgx.Tx) error {
		_, err := tx.CopyFrom(ctx,
			pgx.Identifier{tmpTable},
			[]string{"jobset_id", "seqno", "update_time"},
			pgx.CopyFromSlice(len(seqNos), func(i int) ([]interface{}, error) {
				return []interface{}{
					seqNos[i].JobSetId,
					seqNos[i].SeqNo,
					seqNos[i].UpdateTime,
				}, nil
			}),
		)
		return errors.WithStack(err)
	}

	copyToDest := func(tx pgx.Tx) error {
		_, err := tx.Exec(
			ctx,
			fmt.Sprintf(`
					INSERT INTO latest_seqno (jobset_id, seqno, update_time) SELECT * from %s
					ON CONFLICT (jobset_id)
					DO UPDATE SET seqno = EXCLUDED.seqno`, tmpTable),
		)
		return errors.WithStack(err)
	}

	return database.BatchInsert(ctx, e.db, createTmp, insertTmp, copyToDest)
}

// InsertEvents inserts a batch of events into the event table using the copy protocol
// Any existing events will not be overwritten.
func (e *EventDb) InsertEvents(ctx context.Context, events []*model.EventRow) error {
	tmpTable := database.UniqueTableName("event")

	createTmp := func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s 
				(
				  jobset_id  bigint,
				  seqno      bigint,
				  event      bytea
				) ON COMMIT DROP;`, tmpTable))
		return errors.WithStack(err)
	}

	insertTmp := func(tx pgx.Tx) error {
		_, err := tx.CopyFrom(ctx,
			pgx.Identifier{tmpTable},
			[]string{"jobset_id", "seqno", "event"},
			pgx.CopyFromSlice(len(events), func(i int) ([]interface{}, error) {
				return []interface{}{
					events[i].JobSetId,
					events[i].SeqNo,
					events[i].Event,
				}, nil
			}),
		)
		return err
	}

	copyToDest := func(tx pgx.Tx) error {
		_, err := tx.Exec(
			ctx,
			fmt.Sprintf(`
					INSERT INTO event (jobset_id, seqno, event) SELECT * from %s
					ON CONFLICT DO NOTHING`, tmpTable),
		)
		return errors.WithStack(err)
	}

	return database.BatchInsert(ctx, e.db, createTmp, insertTmp, copyToDest)
}

// GetEvents fetches all the events from the database for the list of requests.  If the number of rows returned will
// not exceed the limit supplied.
// This function exists because we expect that at any given point in time there may be new rows available for multiple
// jobsets, each with a relatively small number of rows available.  This allows us to fetch the rows for multiple
// jobsets in  a single query, which should be more efficient than making a large number of queries
func (e *EventDb) GetEvents(requests []*model.EventRequest, limit int) ([]*model.EventResponse, error) {
	// Build up a query of the form:
	// SELECT 1 as reqIdx, sequence, payload
	// 	FROM events
	// 	WHERE jobset = 'foo' AND sequence > x
	// ORDER BY SEQUENCE LIMIT y
	// UNIONALL
	// SELECT 2 as reqIdx, sequence, payload
	// FROM events
	// WHERE jobset = 'foo' AND sequence > x
	// ORDER BY SEQUENCE LIMIT y
	stmts := make([]string, len(requests))
	params := make([]interface{}, len(requests)*3)
	for i, req := range requests {
		stmts[i] = fmt.Sprintf("("+
			"SELECT %d as reqIdx, seqno, event "+
			"FROM event "+
			"WHERE jobset_id = $%d AND seqno > $%d "+
			"ORDER BY seqno "+
			"LIMIT $%d)", i, i*3+1, (i*3)+2, (i*3)+3)
		params[i*3] = req.Jobset
		params[i*3+1] = req.Sequence
		params[i*3+2] = limit
	}

	query := strings.Join(stmts, " UNION ALL ")
	if len(requests) > 1 {
		query = fmt.Sprintf("%s ORDER BY reqIdx, seqno", query)
	}

	rows, err := e.db.Query(context.Background(), query, params...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// make an array to hold the results
	results := make([]*model.EventResponse, len(requests))
	for i := 0; i < len(requests); i++ {
		results[i] = &model.EventResponse{
			SubscriptionId: requests[i].SubscriptionId,
			Events:         []*model.EventRow{},
		}
	}

	// Parse out results
	var reqIdx int32
	var seqNo int64
	var payload []byte
	for rows.Next() {
		err := rows.Scan(&reqIdx, &seqNo, &payload)
		if err != nil {
			return nil, err
		}
		e := &model.EventRow{
			JobSetId: requests[reqIdx].Jobset,
			SeqNo:    seqNo,
			Event:    payload,
		}
		results[reqIdx].Events = append(results[reqIdx].Events, e)
	}
	return results, nil
}

// LoadJobsetsAfter Loads all Jobsets created the supplied time
func (e *EventDb) LoadJobsetsAfter(ctx context.Context, after time.Time) ([]*model.JobsetRow, error) {
	rows, err := e.db.Query(ctx, "SELECT id, queue, jobset, created FROM jobset WHERE created > $1", after)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	jobsets := make([]*model.JobsetRow, 0)
	for rows.Next() {
		jobset := &model.JobsetRow{}
		err := rows.Scan(&jobset.JobSetId, &jobset.Queue, &jobset.Jobset, &jobset.Created)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		jobsets = append(jobsets, jobset)
	}
	return jobsets, nil
}

// LoadSeqNos Loads all sequence numbers.  This could be very  large so is mainly intended for test purposes
func (e *EventDb) LoadSeqNos(ctx context.Context) ([]*model.SeqNoRow, error) {
	rows, err := e.db.Query(ctx, "SELECT jobset_id, seqno, update_time FROM latest_seqno ORDER BY jobset_id")
	if err != nil {
		return nil, err
	}
	seqNos := make([]*model.SeqNoRow, 0)
	for rows.Next() {
		seqNo := &model.SeqNoRow{}
		err := rows.Scan(&seqNo.JobSetId, &seqNo.SeqNo, &seqNo.UpdateTime)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		seqNos = append(seqNos, seqNo)
	}
	return seqNos, nil
}

// LoadEvents Loads all events.  This could be very large so is mainly intended for test purposes
func (e *EventDb) LoadEvents(ctx context.Context) ([]*model.EventRow, error) {
	rows, err := e.db.Query(ctx, "SELECT jobset_id, seqno, event FROM event")
	if err != nil {
		return nil, err
	}
	events := make([]*model.EventRow, 0)
	for rows.Next() {
		seqNo := &model.EventRow{}
		err := rows.Scan(&seqNo.JobSetId, &seqNo.SeqNo, &seqNo.Event)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		events = append(events, seqNo)
	}
	return events, nil
}

// GetOrCreateJobsetId will retrieve a mapping from (queue, jobset) -> jobsetId or create a new one if one doesn't exist
func (e *EventDb) GetOrCreateJobsetId(ctx context.Context, queue string, jobset string) (int64, error) {
	_, err := e.db.Exec(
		ctx,
		`INSERT INTO jobset (queue, jobset, created) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;`,
		queue,
		jobset,
		time.Now().In(time.UTC))
	if err != nil {
		return 0, errors.WithStack(err)
	}

	var id int64 = 0
	row := e.db.QueryRow(ctx, `SELECT id FROM jobset WHERE queue = $1 AND jobset = $2`, queue, jobset)
	err = row.Scan(&id)
	return id, errors.WithStack(err)
}

// loadSequenceNosForIds will load all the sequence numbers for the supplied jobsetIds
func loadSequenceNosForIds(ctx context.Context, querier pgxtype.Querier, jobsetIds []int64) (map[int64]int64, error) {
	rows, err := querier.Query(ctx, "SELECT jobset_id, seqno FROM latest_seqno WHERE jobset_id = any($1)", jobsetIds)
	if err != nil {
		return nil, err
	}
	seqnos := make(map[int64]int64, 0)
	for rows.Next() {
		var jobsetId int64 = -1
		var seqno int64 = -1
		err := rows.Scan(&jobsetId, &seqno)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		seqnos[jobsetId] = seqno
	}
	return seqnos, nil
}

// lastSeqNumbers determines the last sequence number for each jobset in the input event rows
// this function  assumes that the input event rows are already sorted by seq number
func lastSeqNumbers(events []*model.EventRow) []*model.SeqNoRow {
	seqNosById := make(map[int64]int64)

	for _, event := range events {
		seqNosById[event.JobSetId] = event.SeqNo
	}

	seqNos := make([]*model.SeqNoRow, 0)
	for jobSet, seqNo := range seqNosById {
		seqNos = append(seqNos, &model.SeqNoRow{
			JobSetId:   jobSet,
			SeqNo:      seqNo,
			UpdateTime: time.Now().In(time.UTC),
		})
	}
	return seqNos
}

// Returns the distinct jobset ids in each supplied batch of events
// Note that we have  to check whether any given event is nil
// As this will occur if we've had a failed message
func distinctJobsets(events []*model.EventRow) []int64 {
	keys := make(map[int64]bool)
	distinctJobsets := make([]int64, 0)
	for _, e := range events {
		if e != nil {
			if _, value := keys[e.JobSetId]; !value {
				keys[e.JobSetId] = true
				distinctJobsets = append(distinctJobsets, e.JobSetId)
			}
		}
	}
	return distinctJobsets
}
