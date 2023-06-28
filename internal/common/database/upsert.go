package database

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

func UpsertWithTransaction[T any](ctx context.Context, db *pgxpool.Pool, tableName string, records []T) error {
	if len(records) == 0 {
		return nil
	}
	return db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		return Upsert(ctx, tx, tableName, records)
	})
}

// Upsert is an optimised SQL call for bulk upserts.
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
func Upsert[T any](ctx context.Context, tx pgx.Tx, tableName string, records []T) error {
	if len(records) < 1 {
		return nil
	}

	// Write records into postgres.
	// First, create a temporary table for loading data in bulk using the copy protocol.
	// TODO: don't use select * here but rather just select the cols we care about
	tempTableName := uniqueTableName(tableName)
	_, err := tx.Exec(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s ON COMMIT DROP AS SELECT * FROM %s LIMIT 0;", tempTableName, tableName))
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

func uniqueTableName(table string) string {
	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	return fmt.Sprintf("%s_tmp_%s", table, suffix)
}
