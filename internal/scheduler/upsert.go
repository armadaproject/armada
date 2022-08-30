package scheduler

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/severinson/pulsar-client-go/pulsar"

	"github.com/G-Research/armada/internal/pulsarutils"
)

// ErrStaleWrite is returned by UpsertRecords if the db already contains
// data more recent than what we're trying to write.
//
// Contains a StaleWrite struct for each partition for which the write is stale.
// Since a write may be based of data from several partition of a topic.
type ErrStaleWrite struct {
	Topic       string
	StaleWrites []StaleWrite
}

// StaleWrite contains the message id associated with the write and
// the message id already stored in the database for a particular (partition of a) topic.
type StaleWrite struct {
	// Message id of the most recent message attempted to write.
	WriteMessageId pulsar.MessageID
	// Message id stored in the database.
	DbMessageId pulsar.MessageID
}

func (err *ErrStaleWrite) Error() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("stale write for topic '%s'", err.Topic))
	if len(err.StaleWrites) > 0 {
		sb.WriteString(": ")
	}
	for i, staleWrite := range err.StaleWrites {
		sb.WriteString(fmt.Sprintf(
			"%d-th partition write %s is less recent than %s",
			staleWrite.DbMessageId.PartitionIdx(), staleWrite.WriteMessageId, staleWrite.DbMessageId),
		)
		if i != len(err.StaleWrites)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}

// IdempotentUpsert is an optimized SQL call for bulk upserts.
//
// For efficiency, this function:
// 1. Creates an empty temporary SQL table.
// 2. Inserts all records into the temporary table using the postgres-specific COPY wire protocol.
// 3. Upserts all records from the temporary table into the target table (as specified by tableName).
//
// The COPY protocol can be faster than repeated inserts for as little as 5 rows; see
// https://www.postgresql.org/docs/current/populate.html
// https://pkg.go.dev/github.com/jackc/pgx/v4#hdr-Copy_Protocol
//
// The records to write should be structs with fields marked with "db" tags.
// Field names and values are extracted using the NamesValuesFromRecord function;
// see its definition for details. The first field is used as the primary key in SQL.
//
// The temporary table is created with the provided schema, which should be of the form
// (
//
//	id UUID PRIMARY KEY,
//	width int NOT NULL,
//	height int NOT NULL
//
// )
// I.e., it should omit everything before and after the "(" and ")", respectively.
//
// This function relies on Pulsar message ids for idempotent writes;
// if a Pulsar message id stored in postgres is more recent than one provided to this function,
// the transaction is rolled back and a ErrStaleWrite error is returned.
//
// This function assumes all records are derived from a single (possibly partitioned) Pulsar topic
// with name topicName. writeMessageIds maps partition indices to the id of the most recently
// received Pulsar message for that partition.
func IdempotentUpsert(ctx context.Context, db *pgxpool.Pool, topicName string, writeMessageIds map[int32]pulsar.MessageID, tableName string, schema string, records []interface{}) error {
	if len(records) == 0 {
		return nil
	}
	return db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {

		err := IdempotencyCheck(ctx, tx, topicName, writeMessageIds)
		if err != nil {
			return err
		}

		err = CopyProtocolUpsert(ctx, tx, tableName, schema, records)
		if err != nil {
			return err
		}

		return nil
	})
}

// Upsert is like [IdempotentUpsert], except the it does no idempotency check.
func Upsert(ctx context.Context, db *pgxpool.Pool, tableName string, schema string, records []interface{}) error {
	if len(records) == 0 {
		return nil
	}
	return db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		return CopyProtocolUpsert(ctx, tx, tableName, schema, records)
	})
}

func IdempotencyCheck(ctx context.Context, tx DBTX, topicName string, writeMessageIds map[int32]pulsar.MessageID) error {

	// Load the message ids stored in postgres.
	queries := New(tx)
	sqlWriteMessageIds, err := queries.GetTopicMessageIds(ctx, topicName)
	if err != pgx.ErrNoRows && err != nil {
		return errors.WithStack(err)
	}

	// Check if the data stored in postgres is more recent than what we're trying to write.
	staleWrites := make([]StaleWrite, 0)
	for _, sqlWriteMessageId := range sqlWriteMessageIds {

		// Convert from the SQL-specific representation of message ids to one that can be used for comparison.
		dbMessageId := pulsarMessageIdFromPulsarRecord(sqlWriteMessageId)

		// If the id loaded from the database is more recent than the one provided, abort the transaction.
		// Since the data we're trying to write is stale.
		if writeMessageId, ok := writeMessageIds[dbMessageId.PartitionIdx()]; ok {
			isGreater, err := dbMessageId.Greater(writeMessageId)
			if err != nil {
				return err
			}
			if isGreater {
				staleWrites = append(staleWrites, StaleWrite{
					WriteMessageId: writeMessageId,
					DbMessageId:    dbMessageId,
				})
			}
		}
	}
	if len(staleWrites) > 0 {
		return &ErrStaleWrite{
			Topic:       topicName,
			StaleWrites: staleWrites,
		}
	}

	// Otherwise, we have more recent data than what is already stored in the db that we should write.
	// Update the message id in postgres to reflect the data to be written.
	//
	// TODO: Add a call to insert these in a single call.
	for partitionIdx, writeMessageId := range writeMessageIds {
		err = queries.UpsertMessageId(ctx, UpsertMessageIdParams{
			Topic:        fmt.Sprintf("%s-partition-%d", topicName, partitionIdx),
			LedgerID:     writeMessageId.LedgerID(),
			EntryID:      writeMessageId.EntryID(),
			BatchIdx:     writeMessageId.BatchIdx(),
			PartitionIdx: writeMessageId.PartitionIdx(),
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func CopyProtocolUpsert(ctx context.Context, tx pgx.Tx, tableName string, schema string, records []interface{}) error {

	// Write records into postgres.
	// First, create a temporary table for loading data in bulk using the copy protocol.
	// The table is created with the provided schema.
	tempTableName := "insert" // TODO: Is this name unique per transaction?
	_, err := tx.Exec(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s %s ON COMMIT DROP;", tempTableName, schema))
	if err != nil {
		return errors.WithStack(err)
	}

	// Use the postgres-specific COPY wire protocol to load data into the new table in a single operation.
	// The COPY protocol can be faster than repeated inserts for as little as 5 rows; see
	// https://www.postgresql.org/docs/current/populate.html
	// https://pkg.go.dev/github.com/jackc/pgx/v4#hdr-Copy_Protocol
	//
	// We're guaranteed there is at least one record.
	names, _ := NamesValuesFromRecord(records[0])
	if len(names) < 2 {
		return errors.Errorf("Names() must return at least 2 elements, but got %v", names)
	}
	n, err := tx.CopyFrom(ctx,
		pgx.Identifier{tempTableName},
		names,
		pgx.CopyFromSlice(len(records), func(i int) ([]interface{}, error) {
			// TODO: Are we guaranteed that values always come in the order listed in the record? Otherwise we need to control the order.
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
	for i, name := range names {
		fmt.Fprintf(&b, "%s = EXCLUDED.%s", name, name)
		if i != len(names)-1 {
			fmt.Fprintf(&b, ", ")
		}
	}
	fmt.Fprint(&b, ";")
	_, err = tx.Exec(ctx, b.String())
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func pulsarMessageIdFromPulsarRecord(pulsarRecord Pulsar) *pulsarutils.PulsarMessageId {
	return pulsarutils.New(
		pulsarRecord.LedgerID,
		pulsarRecord.EntryID,
		pulsarRecord.PartitionIdx,
		pulsarRecord.BatchIdx,
	)
}

// NamesFromRecord returns a slice composed of the field names in a struct marked with "db" tags.
//
// For example, if x is an instance of a struct with definition
//
//	type Rectangle struct {
//		Width int  `db:"width"`
//		Height int `db:"height"`
//	},
//
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
//
//	type Rectangle struct {
//	 Name string,
//		Width int  `db:"width"`
//		Height int `db:"height"`
//	},
//
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
//
//	type Rectangle struct {
//		Width int  `db:"width"`
//		Height int `db:"height"`
//	},
//
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
