package eventdb

import (
	"context"
	"fmt"
	"github.com/G-Research/armada/internal/eventingester/model"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"strings"
)

func Insert(ctx context.Context, db *pgxpool.Pool, events []*model.EventMessage) error {

	if len(offsets) < 1 {
		return nil
	}

	// filter out any messages earlier than last read offsets

	// Resolve Jobset Ids

	// Replace queue and jobsets with jobset ids

	// Inset rows

	// Insert offsets

	// Insert partition indexes

}

func InsertPartitionsBatch(ctx context.Context, db *pgxpool.Pool, messageIDs []pulsar.MessageID) error {

	tmpTable := uniqueTableName("last_processed_message")

	createTmp := func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s 
				(
				  partition	   int,
				  ledger_id    bigint,
                  entry_id     bigint
				  batch_idx    int,
				) ON COMMIT DROP;`, tmpTable))
		return err
	}

	insertTmp := func(tx pgx.Tx) error {
		_, err := tx.CopyFrom(ctx,
			pgx.Identifier{tmpTable},
			[]string{"partition", "ledger_id", "entry_id", "batch_idx"},
			pgx.CopyFromSlice(len(messageIDs), func(i int) ([]interface{}, error) {
				return []interface{}{
					messageIDs[i].PartitionIdx(),
					messageIDs[i].LedgerID(),
					messageIDs[i].EntryID(),
					messageIDs[i].BatchIdx(),
				}, nil
			}),
		)
		return err
	}

	copyToDest := func(tx pgx.Tx) error {
		_, err := tx.Exec(
			ctx,
			fmt.Sprintf(`
					INSERT INTO last_processed_message (partition, ledger_id, entry_id, batch_idx SELECT * from %s
					ON CONFLICT DO NOTHING`, tmpTable),
		)
		return err
	}

	return batchInsert(ctx, db, createTmp, insertTmp, copyToDest)
}

func InsertOffsetsBatch(ctx context.Context, db *pgxpool.Pool, offsets []*model.Offset) error {
	tmpTable := uniqueTableName("offset")

	createTmp := func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s 
				(
				  jobset_id 	bigint,
				  offset    bigint,
				  last_update     timestamp,
				) ON COMMIT DROP;`, tmpTable))
		return err
	}

	insertTmp := func(tx pgx.Tx) error {
		_, err := tx.CopyFrom(ctx,
			pgx.Identifier{tmpTable},
			[]string{"jobset_id", "offset", "last_update"},
			pgx.CopyFromSlice(len(offsets), func(i int) ([]interface{}, error) {
				return []interface{}{
					offsets[i].JobSetId,
					offsets[i].Offset,
					offsets[i].LastUpdate,
				}, nil
			}),
		)
		return err
	}

	copyToDest := func(tx pgx.Tx) error {
		_, err := tx.Exec(
			ctx,
			fmt.Sprintf(`
					INSERT INTO offset (jobset_id, offset, last_update SELECT * from %s
					ON CONFLICT DO NOTHING`, tmpTable),
		)
		return err
	}

	return batchInsert(ctx, db, createTmp, insertTmp, copyToDest)
}

func InsertJobsetsBatch(ctx context.Context, db *pgxpool.Pool, jobsets []*model.JobsetRow) error {
	tmpTable := uniqueTableName("jobset")

	createTmp := func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s 
				(
				  jobset_id 	bigint,
				  queue    varchar(512),
				  jobset     varchar(512),
				) ON COMMIT DROP;`, tmpTable))
		return err
	}

	insertTmp := func(tx pgx.Tx) error {
		_, err := tx.CopyFrom(ctx,
			pgx.Identifier{tmpTable},
			[]string{"jobset_id", "queue", "jobset"},
			pgx.CopyFromSlice(len(jobsets), func(i int) ([]interface{}, error) {
				return []interface{}{
					jobsets[i].JobSetId,
					jobsets[i].Queue,
					jobsets[i].Jobset,
				}, nil
			}),
		)
		return err
	}

	copyToDest := func(tx pgx.Tx) error {
		_, err := tx.Exec(
			ctx,
			fmt.Sprintf(`
					INSERT INTO jobset (jobset_id, queue, jobset SELECT * from %s
					ON CONFLICT DO NOTHING`, tmpTable),
		)
		return err
	}

	return batchInsert(ctx, db, createTmp, insertTmp, copyToDest)
}

func InsertEventsBatch(ctx context.Context, db *pgxpool.Pool, events []*model.EventRow) error {

	tmpTable := uniqueTableName("events")

	createTmp := func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
				CREATE TEMPORARY TABLE %s 
				(
				  jobset_id 	bigint,
				  offset     bigint,
				  event     bytea,
				) ON COMMIT DROP;`, tmpTable))
		return err
	}

	insertTmp := func(tx pgx.Tx) error {
		_, err := tx.CopyFrom(ctx,
			pgx.Identifier{tmpTable},
			[]string{"jobset_id", "offset", "event"},
			pgx.CopyFromSlice(len(events), func(i int) ([]interface{}, error) {
				return []interface{}{
					events[i].JobSetId,
					events[i].Index,
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
					INSERT INTO event (jobset_id, offset, event SELECT * from %s
					ON CONFLICT DO NOTHING`, tmpTable),
		)
		return err
	}

	return batchInsert(ctx, db, createTmp, insertTmp, copyToDest)
}

func uniqueTableName(table string) string {
	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	return fmt.Sprintf("%s_tmp_%s", table, suffix)
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

func LastOffsets(events []*model.EventMessage) []*model.Offset {

	offsetsById := make(map[string]pulsar.MessageID)

	for _, event := range events {
		offsetsById[event.Event.JobSetName] = event.MessageId
	}

	offsets := make([]*model.Offset)
	for _, event := range offsetsById {
		offsetsById[event.Event.JobSetName] = event.MessageId
	}
}
