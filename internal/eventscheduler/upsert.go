package state

import (
	"context"
	"fmt"
	"github.com/G-Research/log-transition-prototype/internal/transitions/messageid"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"

	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"

	"github.com/G-Research/log-transition-prototype/internal/transitions"
)

// This file contains hand-written code relating to database (i.e., state) operations.
// As opposed to db.go, models.go, and query.sql.go, which are code-generated from the files in the sql sub-directory.

func ProtoRecordFromSqlRecord(sqlRecord *Record) (*transitions.Record, error) {
	return &transitions.Record{
		Id:      sqlRecord.ID.String(),
		Value:   sqlRecord.Value,
		Payload: sqlRecord.Payload,
	}, nil
}

func SqlRecordFromProtoRecord(protoRecord *transitions.Record) (Record, error) {
	id, err := uuid.Parse(protoRecord.Id)
	if err != nil {
		return Record{}, err
	}
	return Record{
		ID:      id,
		Value:   protoRecord.Value,
		Payload: protoRecord.Payload,
	}, nil
}

// ErrStaleWrite is a custom error type returned by UpsertRecords if the db already contains
// more recent data than what we're trying to write.
type ErrStaleWrite struct {
	// Message id of the most recent message attempted to write.
	writeMessageId pulsar.MessageID
	// Message id stored in the database.
	dbMessageId pulsar.MessageID
}

func (err *ErrStaleWrite) Error() string {
	return fmt.Sprintf("stale write: id %s is less recent than %s", err.writeMessageId, err.dbMessageId)
}

// UpsertRecords is an optimized hand-written SQL call for upserting many records in a single operation.
// TODO: We're using Sprintf to create SQL commands. Surely there's a better way.
// TODO: I didn't get using UUIDs as table names to work. Maybe because there are rules for how table names must be formatted.
func UpsertRecords(ctx context.Context, db *pgx.Conn, writeMessageId pulsar.MessageID, records []Record) error {

	return db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {

		// Load the message id stored in the sql database
		// We refer to the right message id instance by a uuid.
		queries := New(tx)
		topicName := "topic"
		//uuid, err := uuid.Parse("5af412bf-f412-420a-bf64-ab366fc0b7e6")
		//if err != nil {
		//	return err
		//}
		dbHasId := true
		sqlWriteMessageId, err := queries.GetMessageId(ctx, topicName)
		if err == pgx.ErrNoRows {
			dbHasId = false
		} else if err != nil {
			return err
		}
		dbMessageId := messageid.New(
			sqlWriteMessageId.Ledgerid,
			sqlWriteMessageId.Entryid,
			sqlWriteMessageId.Partitionidx,
			sqlWriteMessageId.Batchidx,
			)

		// If the id loaded from the database is more recent than the one provided, we abort the transaction,
		// since the database contains data more recent than what we're trying to write.
		if dbHasId && !messageid.ComesBefore(dbMessageId, writeMessageId) {
			return &ErrStaleWrite{
				writeMessageId: writeMessageId,
				dbMessageId: dbMessageId,
			}
		}

		// Otherwise we have more recent data than what is already stored in the db, which we should write.
		// We also update the message id stored in the db to reflect these writes.
		err = queries.UpsertMessageId(ctx, UpsertMessageIdParams{
			Topic:        topicName,
			Ledgerid:     writeMessageId.LedgerID(),
			Entryid:      writeMessageId.EntryID(),
			Batchidx:     writeMessageId.BatchIdx(),
			Partitionidx: writeMessageId.PartitionIdx(),
		})
		if err != nil {
			return err
		}

		// First, create a temporary table for loading data into in bulk.
		//
		// To make this function thread-safe, randomly generate a new table name at each invocation.
		// This table is automatically dropped at the end of the transaction using the "ON COMMIT" instruction.
		// Because tables names must start with an alphabetic character, we prepend an "A".
		// tempTable := "A" + shortuuid.New()
		tempTable := "foobar"
		_, err = tx.Exec(ctx, fmt.Sprintf(`
CREATE TEMPORARY TABLE %s (
id uuid PRIMARY KEY,
value integer NOT NULL,
payload text NOT NULL,
deleted boolean NOT NULL
) ON COMMIT DROP;
`, tempTable))
		if err != nil {
			return err
		}

		// Use the postgres-specific COPY wire protocol to load data into the new table in a single operation.
		// The COPY protocol can be faster than repeated inserts for as little as 5 rows; see
		// https://www.postgresql.org/docs/current/populate.html
		// https://pkg.go.dev/github.com/jackc/pgx/v4#hdr-Copy_Protocol
		n, err := tx.CopyFrom(ctx,
			pgx.Identifier{tempTable},
			[]string{"id", "value", "payload", "deleted"},
			pgx.CopyFromSlice(len(records), func(i int) ([]interface{}, error) {
				// Postgres expects UUIDs to be encoded in binary format.
				//binaryUUID, err := records[i].ID.MarshalBinary()
				//if err != nil {
				//	return nil, err
				//}
				return []interface{}{records[i].ID, records[i].Value, records[i].Payload, records[i].Deleted}, nil
			}),
		)
		if err != nil {
			return err
		}
		if n != int64(len(records)) {
			return errors.Errorf("only %d out of %d rows were inserted", n, len(records))
		}

		// Move those rows into the main table, using ON CONFLICT rules to over-write existing rows.
		_, err = tx.Exec(ctx, fmt.Sprintf(`
INSERT INTO records SELECT * from %s
ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value, payload = EXCLUDED.payload, deleted = EXCLUDED.deleted; 
`, tempTable))
		if err != nil {
			return err
		}

		return nil
	})
}
