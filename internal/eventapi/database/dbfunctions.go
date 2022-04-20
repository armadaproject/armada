package database

package eventsdb

import (
	"PostgresPoc/pkg/models"
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)


func InsertRecords(ctx context.Context, db *pgxpool.Pool, records []models.Event) error {

	return db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {

		// Use the postgres-specific COPY wire protocol to load data into the new table in a single operation.
		// The COPY protocol can be faster than repeated inserts for as little as 5 rows; see
		// https://www.postgresql.org/docs/current/populate.html
		// https://pkg.go.dev/github.com/jackc/pgx/v4#hdr-Copy_Protocol
		n, err := tx.CopyFrom(ctx,
			pgx.Identifier{"events"},
			[]string{"jobset", "sequence", "payload", "ts"},
			pgx.CopyFromSlice(len(records), func(i int) ([]interface{}, error) {
				// Postgres expects UUIDs to be encoded in binary format.
				binaryUUID, err := records[i].Jobset.MarshalBinary()
				if err != nil {
					return nil, err
				}
				return []interface{}{binaryUUID, records[i].Sequence, records[i].Payload, records[i].Ts}, nil
			}),
		)
		if err != nil {
			return err
		}
		if n != int64(len(records)) {
			return errors.Errorf("only %d out of %d rows were inserted", n, len(records))
		}
		return nil
	})
}

func DeleteRecordsSince(db *pgxpool.Pool, ts time.Time) error  {
	stmt := "DELETE FROM events WHERE ts < $1"
	start := time.Now()
	_, err := db.Exec(context.Background(), stmt, ts)
	taken := time.Now().Sub(start)
	log.Infof("Ran cleanup job in %dms", taken.Milliseconds())
	return err
}

func GetEvents(db *pgxpool.Pool, requests []UpdateRequest) ([]UpdateResponse, error) {

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
		stmts[i] = fmt.Sprintf("(" +
			"SELECT %d as reqIdx, sequence, payload " +
			"FROM events " +
			"WHERE jobset = $%d AND sequence > $%d " +
			"ORDER BY SEQUENCE " +
			"LIMIT 100000)", i, i*2 + 1, (i*2) + 2)
		params[i*2] = req.Jobset
		params[i*2+1] = req.Sequence
	}

	query := strings.Join(stmts, " UNION ALL ");
	if len(requests) > 1 {
		query = fmt.Sprintf("%s ORDER BY reqIdx, SEQUENCE", query)
	}

	rows, err := db.Query(context.Background(), query, params...)
	if err != nil {
		return nil, err
	}

	// make an array to hold the results
	results := make([]UpdateResponse, len(requests))
	for i:= 0; i< len(requests); i++{
		results[i] = UpdateResponse{
			SubscriptionId: requests[i].SubscriptionId,
			Events:         []models.Event{},
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
		e := models.Event{
			Jobset:   requests[reqIdx].Jobset,
			Sequence: sequence,
			Payload:  payload,
		}
		// TODO: what's the performance of append here?
		results[reqIdx].Events = append(results[reqIdx].Events, e)
	}
	return results, nil
}
