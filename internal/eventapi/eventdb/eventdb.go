package eventdb

import (
	"context"
	"fmt"
	"github.com/G-Research/armada/internal/eventapi/model"
	"github.com/google/uuid"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

type EventDb struct {
	db *pgxpool.Pool
}

func NewEventDb(db *pgxpool.Pool) *EventDb {
	return &EventDb{
		db: db,
	}
}

func (e *EventDb) UpdateEvents(ctx context.Context, events []*model.EventRow) error {

	// If there's nothing to insert then we can just return
	if len(events) < 1 {
		return nil
	}

	return e.db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {

		// First filter out any events with  sequenceNo equal to or lower than sequenceNos we already have
		jobsetId := distinctJobsets(events)
		sequenceNos, err := loadSequenceNosForIds(ctx, tx, jobsetId)
		if err != nil {
			return err
		}
		eventsToInsert := make([]*model.EventRow, 0)
		for _, event := range events {
			lastProcessed, seqNoFound := sequenceNos[event.JobSetId]
			if !seqNoFound || lastProcessed < event.SeqNo {
				eventsToInsert = append(eventsToInsert, event)
			}
		}

		// Now insert the Events
		err = e.InsertEvents(ctx, eventsToInsert)
		if err != nil {
			return err
		}

		// Finally insert the seqnos corresponding to those events
		seqNosToInsert := lastSeqNumbers(events)
		err = e.InsertSeqNos(ctx, seqNosToInsert)
		return err
	})
}

func (e *EventDb) InsertSeqNos(ctx context.Context, seqNos []*model.SeqNoRow) error {
	tmpTable := uniqueTableName("latest_seqno")

	createTmp := func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s 
				(
				  jobset_id 	bigint,
				  seqno         bigint,
				  update_time   timestamp
				) ON COMMIT DROP;`, tmpTable))
		return err
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
		return err
	}

	copyToDest := func(tx pgx.Tx) error {
		_, err := tx.Exec(
			ctx,
			fmt.Sprintf(`
					INSERT INTO latest_seqno (jobset_id, seqno, update_time) SELECT * from %s
					ON CONFLICT (jobset_id)
					DO UPDATE SET seqno = EXCLUDED.seqno`, tmpTable),
		)
		return err
	}

	return batchInsert(ctx, e.db, createTmp, insertTmp, copyToDest)
}

func uniqueTableName(table string) string {
	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	return fmt.Sprintf("%s_tmp_%s", table, suffix)
}

func (e *EventDb) InsertEvents(ctx context.Context, events []*model.EventRow) error {

	tmpTable := uniqueTableName("event")

	createTmp := func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s 
				(
				  jobset_id  bigint,
				  seqno      bigint,
				  event      bytea
				) ON COMMIT DROP;`, tmpTable))
		return err
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
		return err
	}

	return batchInsert(ctx, e.db, createTmp, insertTmp, copyToDest)
}

func (e *EventDb) LoadJobsetsAfter(ctx context.Context, after time.Time) ([]*model.JobsetRow, error) {
	rows, err := e.db.Query(ctx, "SELECT id, queue, jobset, created FROM jobset WHERE created > $1", after)
	if err != nil {
		return nil, err
	}
	jobsets := make([]*model.JobsetRow, 0)
	for rows.Next() {
		jobset := &model.JobsetRow{}
		err := rows.Scan(&jobset.JobSetId, &jobset.Queue, &jobset.Jobset, &jobset.Created)
		if err != nil {
			return nil, err
		}
		jobsets = append(jobsets, jobset)
	}
	return jobsets, nil
}

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
			return nil, err
		}
		seqNos = append(seqNos, seqNo)
	}
	return seqNos, nil
}

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
			return nil, err
		}
		events = append(events, seqNo)
	}
	return events, nil
}

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
			return nil, err
		}
		seqnos[jobsetId] = seqno
	}
	return seqnos, nil
}

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

func (e *EventDb) GetOrCreateJobsetId(ctx context.Context, queue string, jobset string) (int64, error) {

	_, err := e.db.Exec(
		ctx,
		`INSERT INTO jobset (queue, jobset, created) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;`,
		queue,
		jobset,
		time.Now().In(time.UTC))

	if err != nil {
		return 0, err
	}

	var id int64 = 0
	row := e.db.QueryRow(ctx, `SELECT id FROM jobset WHERE queue = $1 AND jobset = $2`, queue, jobset)
	err = row.Scan(&id)
	return id, err
}

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

func distinctJobsets(events []*model.EventRow) []int64 {
	keys := make(map[int64]bool)
	distinctJobsets := make([]int64, 0)
	for _, e := range events {
		if _, value := keys[e.JobSetId]; !value {
			keys[e.JobSetId] = true
			distinctJobsets = append(distinctJobsets, e.JobSetId)
		}
	}
	return distinctJobsets
}

func batchInsert(ctx context.Context, db *pgxpool.Pool, createTmp func(pgx.Tx) error,
	insertTmp func(pgx.Tx) error, copyToDest func(pgx.Tx) error) error {

	return db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {

		// Create a temporary table to hold the staging data
		err := createTmp(tx)
		if err != nil {
			return err
		}

		err = insertTmp(tx)
		if err != nil {
			return err
		}

		err = copyToDest(tx)
		if err != nil {
			return err
		}
		return nil
	})
}
