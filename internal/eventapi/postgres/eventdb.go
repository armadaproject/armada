package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/G-Research/armada/internal/eventapi"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type EventDb struct {
	db *pgxpool.Pool
}

func (e *EventDb) InsertRecords(ctx context.Context, records []eventapi.Event) error {

	return e.db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		_, err := tx.CopyFrom(ctx,
			pgx.Identifier{"events"},
			[]string{"jobset", "sequence", "payload"},
			pgx.CopyFromSlice(len(records), func(i int) ([]interface{}, error) {
				return []interface{}{records[i].Jobset, records[i].Sequence, records[i].Payload}, nil
			}),
		)
		return err
	})
}

func (e *EventDb) GetOrCreateJobsetId(ctx context.Context, queue string, jobset string) (int64, error) {

	_, err := e.db.Exec(
		ctx,
		`INSERT INTO jobset (queue, jobset)
				SELECT queue, jobset
				FROM jobset
				WHERE NOT EXISTS (
					SELECT 1
				FROM jobset
				WHERE queue = $1 AND jobset = $2
				);`,
		queue,
		jobset)

	if err != nil {
		return 0, err
	}

	var id = 0
	row := e.db.QueryRow(ctx, `SELECT id FROM jobset WHERE queue = $1 AND jobset = $2`)
	err = row.Scan(&id)
	return 0, err
}

func (e *EventDb) GetJobsets(ctx context.Context) ([]*eventapi.JobSet, error) {
	rows, err := e.db.Query(ctx, `SELECT id, queue, jobset FROM jobset`)
	if err != nil {
		return nil, err
	}

	// Parse out results
	results := make([]*eventapi.JobSet, 0)
	for rows.Next() {
		js := eventapi.JobSet{}
		err := rows.Scan(&js.Id, &js.Queue, &js.Jobset)
		if err != nil {
			return nil, err
		}
		results = append(results, &js)
	}
	return results, nil
}

func (e *EventDb) GetEvents(requests []*eventapi.EventRequest) ([]*eventapi.EventResponse, error) {

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
	params := make([]interface{}, len(requests)*2)
	for i, req := range requests {
		stmts[i] = fmt.Sprintf("("+
			"SELECT %d as reqIdx, sequence, payload "+
			"FROM events "+
			"WHERE jobset = $%d AND sequence > $%d "+
			"ORDER BY SEQUENCE "+
			"LIMIT 100000)", i, i*2+1, (i*2)+2)
		params[i*2] = req.Jobset
		params[i*2+1] = req.Sequence
	}

	query := strings.Join(stmts, " UNION ALL ")
	if len(requests) > 1 {
		query = fmt.Sprintf("%s ORDER BY reqIdx, SEQUENCE", query)
	}

	rows, err := e.db.Query(context.Background(), query, params...)
	if err != nil {
		return nil, err
	}

	// make an array to hold the results
	results := make([]*eventapi.EventResponse, len(requests))
	for i := 0; i < len(requests); i++ {
		results[i] = &eventapi.EventResponse{
			SubscriptionId: requests[i].SubscriptionId,
			Events:         []*eventapi.Event{},
		}
	}

	// Parse out results
	var reqIdx int32
	var sequence int64
	var payload []byte
	for rows.Next() {
		err := rows.Scan(&reqIdx, &sequence, &payload)
		if err != nil {
			return nil, err
		}
		e := &eventapi.Event{
			Jobset:   requests[reqIdx].Jobset,
			Sequence: sequence,
			Payload:  payload,
		}
		results[reqIdx].Events = append(results[reqIdx].Events, e)
	}
	return results, nil
}
