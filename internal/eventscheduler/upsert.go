package eventscheduler

import (
	"context"
	"fmt"
	"strings"

	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

// func ProtoRecordFromSqlRecord(sqlRecord *Record) (*transitions.Record, error) {
// 	return &transitions.Record{
// 		Id:      sqlRecord.ID.String(),
// 		Value:   sqlRecord.Value,
// 		Payload: sqlRecord.Payload,
// 	}, nil
// }

// func SqlRecordFromProtoRecord(protoRecord *transitions.Record) (Record, error) {
// 	id, err := uuid.Parse(protoRecord.Id)
// 	if err != nil {
// 		return Record{}, err
// 	}
// 	return Record{
// 		ID:      id,
// 		Value:   protoRecord.Value,
// 		Payload: protoRecord.Payload,
// 	}, nil
// }

// UpsertRecord representes a type that can be bulk-upserted using the postgres COPY protocol.
type UpsertRecord interface {
	// Names returns the field names. Must be of at least length 2.
	// The first element must be the unique id of this record.
	// For example, ["id", "width", "height"]
	// Names() []string
	// Values returns a slice containing the values of this record.
	// Must be of length equal to the slice returned by Names().
	// For example, [0, 10, 20].
	// Values() ([]interface{}, error)
	// Schema returns the string representation of this record's schema.
	// For example:
	// (
	//	id int PRIMARY KEY,
	//  width int NOT NULL,
	//  height int NOT NULL,
	// )
	Schema() string
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
func UpsertRecords(ctx context.Context, db *pgx.Conn, writeMessageId *pulsarutils.PulsarMessageId, tableName string, topicName string, schema string, records []interface{}) error {
	if len(records) == 0 {
		return nil
	}
	return db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {

		// Load the message id stored in the sql database
		// We refer to the right message id instance by a uuid.
		queries := New(tx)
		dbHasId := true
		sqlWriteMessageId, err := queries.GetMessageId(ctx, topicName)
		if err == pgx.ErrNoRows {
			dbHasId = false
		} else if err != nil {
			return errors.WithStack(err)
		}
		dbMessageId := pulsarMessageIdFromPulsarRecord(sqlWriteMessageId)
		// dbMessageId := pulsarutils.New(
		// 	sqlWriteMessageId.Ledgerid,
		// 	sqlWriteMessageId.Entryid,
		// 	sqlWriteMessageId.Partitionidx,
		// 	sqlWriteMessageId.Batchidx,
		// )

		// If the id loaded from the database is at least as recent as the one provided, abort the transaction.
		// Since the data we're trying to write is stale.
		isGreaterEqual, err := dbMessageId.GreaterEqual(writeMessageId)
		if err != nil {
			return err
		}
		if dbHasId && isGreaterEqual {
			return errors.WithStack(&ErrStaleWrite{
				writeMessageId: writeMessageId,
				dbMessageId:    dbMessageId,
			})
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
			return errors.WithStack(err)
		}

		// Now, write the records into postgres.
		// First, create a temporary table for loading data in bulk using the copy protocol.
		//
		// We're guaranteed there is at least one record.
		//
		// To make this function thread-safe, randomly generate a new table name at each invocation.
		// This table is automatically dropped at the end of the transaction using the "ON COMMIT" instruction.
		// Because tables names must start with an alphabetic character, we prepend an "A".
		// tempTable := "A" + shortuuid.New()
		// 		tempTable := "foobar"
		// 		_, err = tx.Exec(ctx, fmt.Sprintf(`
		// CREATE TEMPORARY TABLE %s (
		// id uuid PRIMARY KEY,
		// value integer NOT NULL,
		// payload text NOT NULL,
		// deleted boolean NOT NULL
		// ) ON COMMIT DROP;
		// `, tempTable))
		// 		if err != nil {
		// 			return err
		// 		}
		tempTableName := "insert"
		_, err = tx.Exec(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s %s ON COMMIT DROP;", tempTableName, schema))
		// _, err = tx.Exec(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s %s ON COMMIT DROP;", tempTableName, records[0].Schema()))
		if err != nil {
			return errors.WithStack(err)
		}

		// Use the postgres-specific COPY wire protocol to load data into the new table in a single operation.
		// The COPY protocol can be faster than repeated inserts for as little as 5 rows; see
		// https://www.postgresql.org/docs/current/populate.html
		// https://pkg.go.dev/github.com/jackc/pgx/v4#hdr-Copy_Protocol
		// names := records[0].Names()
		names, _ := NamesValuesFromRecord(records[0])
		if len(names) < 2 {
			return errors.Errorf("Names() must return at least 2 elements, but got %v", names)
		}
		n, err := tx.CopyFrom(ctx,
			pgx.Identifier{tempTableName},
			names,
			//[]string{"id", "value", "payload", "deleted"},
			pgx.CopyFromSlice(len(records), func(i int) ([]interface{}, error) {
				// Postgres expects UUIDs to be encoded in binary format.
				//binaryUUID, err := records[i].ID.MarshalBinary()
				//if err != nil {
				//	return nil, err
				//}
				// return []interface{}{records[i].ID, records[i].Value, records[i].Payload, records[i].Deleted}, nil
				// return records[i].Values()
				_, values := NamesValuesFromRecord(records[i])
				fmt.Println(values)
				return values, nil
			}),
		)
		if err != nil {
			return errors.WithStack(err)
		}
		if n != int64(len(records)) {
			return errors.Errorf("only %d out of %d rows were inserted", n, len(records))
		}

		// Move those rows into the main table, using ON CONFLICT rules to over-write existing rows.
		// namesWithoutId := names[1:]
		var b strings.Builder
		fmt.Fprintf(&b, "INSERT INTO %s SELECT * from %s ", tableName, tempTableName)
		fmt.Fprintf(&b, "ON CONFLICT (%s) DO UPDATE SET ", names[0])
		for i, name := range names[1:] {
			fmt.Fprintf(&b, "%s = EXCLUDED.%s", name, name)
			fmt.Println(i, " ", len(names))
			if i != len(names)-2 {
				fmt.Fprintf(&b, ", ")
			}
		}
		fmt.Fprint(&b, ";")
		fmt.Println("Copy: ", b.String())
		_, err = tx.Exec(ctx, b.String())
		// 		_, err = tx.Exec(ctx, fmt.Sprintf(`
		// INSERT INTO records SELECT * from %s
		// ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value, payload = EXCLUDED.payload, deleted = EXCLUDED.deleted;
		// `, tempTable))
		if err != nil {
			return errors.WithStack(err)
		}

		return nil
	})
}

func pulsarMessageIdFromPulsarRecord(pulsarRecord Pulsar) *pulsarutils.PulsarMessageId {
	return pulsarutils.New(
		pulsarRecord.Ledgerid,
		pulsarRecord.Entryid,
		pulsarRecord.Partitionidx,
		pulsarRecord.Batchidx,
	)
}
