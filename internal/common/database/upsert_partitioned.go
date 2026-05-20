package database

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

// UpsertPartitionedWithTransaction is the partition-safe sibling of
// UpsertWithTransaction. It stages records via COPY, then performs
// DELETE-by-conflict-key followed by plain INSERT, in a single transaction.
//
// Unlike UpsertWithTransaction, which relies on INSERT ... ON CONFLICT DO UPDATE,
// this variant correctly handles rows that move between partitions (e.g. a row
// whose partition-key column changes between calls). It is intended for tables
// partitioned by a column that forms part of the primary key, where ON CONFLICT
// on the non-partition-key alone is not possible.
//
// conflictKey names the column(s) that identify "the same row" across upserts
// — typically the logical primary key (e.g. ["job_id"]) even when the physical
// primary key is composite (e.g. (job_id, state)). Duplicate conflict-key values
// within a single batch are not deduplicated; callers must dedupe.
func UpsertPartitionedWithTransaction[T any](
	ctx *armadacontext.Context,
	db *pgxpool.Pool,
	tableName string,
	conflictKey []string,
	records []T,
	options ...UpsertOption,
) error {
	if len(records) == 0 {
		return nil
	}
	return pgx.BeginTxFunc(ctx, db, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		return UpsertPartitioned(ctx, tx, tableName, conflictKey, records, options...)
	})
}

// UpsertPartitioned is the in-transaction form of UpsertPartitionedWithTransaction.
// See that function for semantics.
func UpsertPartitioned[T any](
	ctx *armadacontext.Context,
	tx pgx.Tx,
	tableName string,
	conflictKey []string,
	records []T,
	options ...UpsertOption,
) error {
	if len(records) == 0 {
		return nil
	}
	if len(conflictKey) == 0 {
		return errors.New("UpsertPartitioned: conflictKey must contain at least one column")
	}

	opts := &upsertOptions{
		excludeColumns: make(map[string]bool),
	}
	for _, opt := range options {
		opt(opts)
	}

	allNames, _ := NamesValuesFromRecord(records[0])
	writableIndices := make([]int, 0, len(allNames))
	writableNames := make([]string, 0, len(allNames))
	for i, name := range allNames {
		if !opts.excludeColumns[name] {
			writableIndices = append(writableIndices, i)
			writableNames = append(writableNames, name)
		}
	}
	if len(writableNames) < 2 {
		return errors.Errorf("NamesValuesFromRecord must return at least 2 writable columns, got %v", writableNames)
	}

	writableSet := make(map[string]bool, len(writableNames))
	for _, n := range writableNames {
		writableSet[n] = true
	}
	for _, k := range conflictKey {
		if !writableSet[k] {
			return errors.Errorf("UpsertPartitioned: conflict key column %q is not a writable column (present=%v, excluded=%v)", k, allNames, opts.excludeColumns)
		}
	}

	tempTableName := uniqueTableName(tableName)
	_, err := tx.Exec(ctx, fmt.Sprintf(
		"CREATE TEMPORARY TABLE %s ON COMMIT DROP AS SELECT * FROM %s LIMIT 0;",
		tempTableName, tableName))
	if err != nil {
		return errors.WithStack(err)
	}

	n, err := tx.CopyFrom(ctx,
		pgx.Identifier{tempTableName},
		writableNames,
		pgx.CopyFromSlice(len(records), func(i int) ([]interface{}, error) {
			_, allValues := NamesValuesFromRecord(records[i])
			values := make([]interface{}, len(writableIndices))
			for j, idx := range writableIndices {
				values[j] = allValues[idx]
			}
			return values, nil
		}),
	)
	if err != nil {
		return errors.WithStack(err)
	}
	if n != int64(len(records)) {
		return errors.Errorf("only %d out of %d rows were inserted", n, len(records))
	}

	keyList := strings.Join(conflictKey, ",")
	deleteSQL := fmt.Sprintf(
		"DELETE FROM %s WHERE (%s) IN (SELECT %s FROM %s);",
		tableName, keyList, keyList, tempTableName)
	if _, err := tx.Exec(ctx, deleteSQL); err != nil {
		return errors.WithStack(err)
	}

	columnList := strings.Join(writableNames, ",")
	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s;",
		tableName, columnList, columnList, tempTableName)
	if _, err := tx.Exec(ctx, insertSQL); err != nil {
		return errors.WithStack(err)
	}

	return nil
}
