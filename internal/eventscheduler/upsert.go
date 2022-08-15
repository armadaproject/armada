package eventscheduler

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"

	"github.com/G-Research/armada/internal/pulsarutils"
)

type ErrStaleWrites struct {

}

// ErrStaleWrite is a custom error type returned by UpsertRecords if the db already contains
// data more recent than what we're trying to write.
type ErrStaleWrite struct {
	Topic string
	StaleWrites []StaleWrite
}

type StaleWrite struct {
	// Message id of the most recent message attempted to write.
	WriteMessageId pulsar.MessageID
	// Message id stored in the database.
	DbMessageId pulsar.MessageID
}

func (err *ErrStaleWrite) Error() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("stale write for topic %s: ", err.Topic))
	for i, staleWrite := range err.StaleWrites {
		sb.WriteString(fmt.Sprintf("%s is less recent than %s (%d-th partition)"), staleWrite.WriteMessageId, staleWrite.DbMessageId, staleWrite.DbMessageId.PartitionIdx())
		if i != len(err.StaleWrites) - 1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}

// UpsertRecords is an optimized SQL call for upserting many records in a single operation.
//
// For efficiency, this function:
// 1. Creates an empty temporary SQL table.
// 2. Inserts all records into the temporary table using the postgres-specific COPY wire protocol.
// 3. Upserts all records from the temporary table into the table with name tableName.
//
// The COPY protocol can be faster than repeated inserts for as little as 5 rows; see
// https://www.postgresql.org/docs/current/populate.html
// https://pkg.go.dev/github.com/jackc/pgx/v4#hdr-Copy_Protocol
//
// The records to write should be structs with fields marked with "db" tags.
// Field names and values are extracted using the NamesValuesFromRecord function;
// see its definition for details. The first field is assumed to be its unique id.
//
// The temporary table is created with the provided schema, which should be of the form
// (
//   id UUID PRIMARY KEY,
//   width int NOT NULL,
//   height int NOT NULL
// )
// I.e., it should omit everything before and after the "(" and ")", respectively.
//
// This function relies on Pulsar message ids for idempotent writes;
// if a Pulsar message id stored in postgres is more recent than one provided to this function,
// the insert is rolled back and a ErrStaleWrite error is returned.
//
// This function assumes all records are derived from a single (possibly partitioned) Pulsar topic
// with name topicName.  writeMessageIds maps partition indices to the id of the most recently
// received Pulsar message for that partition.
func UpsertRecords(ctx context.Context, db *pgx.Conn, topicName string, writeMessageIds map[int]pulsar.MessageID, tableName string, schema string, records []interface{}) error {
	if len(records) == 0 {
		return nil
	}
	return db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {

		// Load the message ids stored in postgres.
		queries := New(tx)
		dbHasId := true
		sqlWriteMessageIds, err := queries.GetTopicMessageIds(ctx, topicName)
		if err != pgx.ErrNoRows && err != nil {
			return errors.WithStack(err)
		}

		for _, sqlWriteMessageId := range sqlWriteMessageIds {
			dbMessageId := pulsarMessageIdFromPulsarRecord(sqlWriteMessageId)

			// If the id loaded from the database is at least as recent as the one provided, abort the transaction.
			// Since the data we're trying to write is stale (or at least not new).
			if writeMessageId, ok := writeMessageIds[dbMessageId.PartitionIdx()] {
				isGreater, err := dbMessageId.Greater(writeMessageId)
				if err != nil {
					return err
				}

				if isGreater {
					return errors.WithStack(&ErrStaleWrite{
						WriteMessageId: writeMessageId,
						DbMessageId:    dbMessageId,
					})
				}				
			}
		}



		// Otherwise, we have more recent data than what is already stored in the db that we should write.
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
		tempTableName := "insert"
		_, err = tx.Exec(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s %s ON COMMIT DROP;", tempTableName, schema))
		if err != nil {
			return errors.WithStack(err)
		}

		// Use the postgres-specific COPY wire protocol to load data into the new table in a single operation.
		// The COPY protocol can be faster than repeated inserts for as little as 5 rows; see
		// https://www.postgresql.org/docs/current/populate.html
		// https://pkg.go.dev/github.com/jackc/pgx/v4#hdr-Copy_Protocol
		names, _ := NamesValuesFromRecord(records[0])
		if len(names) < 2 {
			return errors.Errorf("Names() must return at least 2 elements, but got %v", names)
		}
		n, err := tx.CopyFrom(ctx,
			pgx.Identifier{tempTableName},
			names,
			pgx.CopyFromSlice(len(records), func(i int) ([]interface{}, error) {
				_, values := NamesValuesFromRecord(records[i])
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
		var b strings.Builder
		fmt.Fprintf(&b, "INSERT INTO %s SELECT * from %s ", tableName, tempTableName)
		fmt.Fprintf(&b, "ON CONFLICT (%s) DO UPDATE SET ", names[0])
		for i, name := range names[1:] {
			fmt.Fprintf(&b, "%s = EXCLUDED.%s", name, name)
			if i != len(names)-2 {
				fmt.Fprintf(&b, ", ")
			}
		}
		fmt.Fprint(&b, ";")
		_, err = tx.Exec(ctx, b.String())
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

// NamesFromRecord returns a slice composed of the field names in a struct marked with "db" tags.
//
// For example, if x is an instance of a struct with definition
// type Rectangle struct {
//	Width int  `db:"width"`
//	Height int `db:"height"`
// },
// it returns ["width", "height"].
func NamesFromRecord(x interface{}) []string {
	t := reflect.TypeOf(x)
	names := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		name := t.Field(i).Tag.Get("db")
		if name != "" {
			names = append(names, name)
		}
	}
	return names
}

// ValuesFromRecord returns a slice composed of the values of the fields in a struct marked with "db" tags.
//
// For example, if x is an instance of a struct with definition
// type Rectangle struct {
//  Name string,
//	Width int  `db:"width"`
//	Height int `db:"height"`
// },
// where Width = 5 and Height = 10, it returns [5, 10].
func ValuesFromRecord(x interface{}) []interface{} {
	t := reflect.TypeOf(x)
	v := reflect.ValueOf(x)
	values := make([]interface{}, 0, v.NumField())
	for i := 0; i < t.NumField(); i++ {
		name := t.Field(i).Tag.Get("db")
		if name != "" {
			value := v.Field(i).Interface()
			values = append(values, value)
		}
	}
	return values
}

// NamesValuesFromRecord returns a slice composed of the field names
// and another composed of the corresponding values
// for fields of a struct marked with "db" tags.
//
// For example, if x is an instance of a struct with definition
// type Rectangle struct {
//	Width int  `db:"width"`
//	Height int `db:"height"`
// },
// where Width = 10 and Height = 5,
// it returns ["width", "height"], [10, 5].
//
// This function does not handle pointers to structs,
// i.e., x must be Rectangle{} and not &Rectangle{}.
func NamesValuesFromRecord(x interface{}) ([]string, []interface{}) {
	t := reflect.TypeOf(x)
	v := reflect.ValueOf(x)
	names := make([]string, 0, t.NumField())
	values := make([]interface{}, 0, v.NumField())
	for i := 0; i < t.NumField(); i++ {
		name := t.Field(i).Tag.Get("db")
		if name != "" {
			names = append(names, name)
			value := v.Field(i).Interface()
			values = append(values, value)
		}
	}
	return names, values
}
