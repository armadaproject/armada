package database

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

type PartitionedRecord struct {
	Id    uuid.UUID `db:"id"`
	State int       `db:"state"`
	Msg   string    `db:"msg"`
}

const partitionedTableName = "partitioned_records"

const partitionedSchema = `
CREATE TABLE partitioned_records (
    id    uuid NOT NULL,
    state int  NOT NULL,
    msg   text NOT NULL,
    PRIMARY KEY (id, state)
) PARTITION BY LIST (state);
CREATE TABLE partitioned_records_p0 PARTITION OF partitioned_records FOR VALUES IN (0);
CREATE TABLE partitioned_records_p1 PARTITION OF partitioned_records FOR VALUES IN (1);
`

func withPartitionedDb(action func(db *pgxpool.Pool) error) error {
	return WithTestDb([]Migration{
		NewMigration(1, "init", partitionedSchema),
	}, action)
}

func selectPartitionedRecords(ctx *armadacontext.Context, db *pgxpool.Pool, table string) ([]PartitionedRecord, error) {
	rows, err := db.Query(ctx, fmt.Sprintf("SELECT id, state, msg FROM %s ORDER BY msg", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PartitionedRecord
	for rows.Next() {
		var r PartitionedRecord
		if err := rows.Scan(&r.Id, &r.State, &r.Msg); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func TestUpsertPartitioned_InitialInsert(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	err := withPartitionedDb(func(db *pgxpool.Pool) error {
		records := []PartitionedRecord{
			{Id: uuid.New(), State: 0, Msg: "a"},
			{Id: uuid.New(), State: 1, Msg: "b"},
			{Id: uuid.New(), State: 0, Msg: "c"},
		}
		err := UpsertPartitionedWithTransaction(ctx, db, partitionedTableName, []string{"id"}, records)
		require.NoError(t, err)

		actual, err := selectPartitionedRecords(ctx, db, partitionedTableName)
		require.NoError(t, err)
		assert.Len(t, actual, 3)

		p0, err := selectPartitionedRecords(ctx, db, "partitioned_records_p0")
		require.NoError(t, err)
		assert.Len(t, p0, 2)

		p1, err := selectPartitionedRecords(ctx, db, "partitioned_records_p1")
		require.NoError(t, err)
		assert.Len(t, p1, 1)
		return nil
	})
	require.NoError(t, err)
}

func TestUpsertPartitioned_CrossPartitionMove(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	err := withPartitionedDb(func(db *pgxpool.Pool) error {
		id := uuid.New()
		initial := []PartitionedRecord{{Id: id, State: 0, Msg: "before"}}
		require.NoError(t, UpsertPartitionedWithTransaction(
			ctx, db, partitionedTableName, []string{"id"}, initial))

		p0, err := selectPartitionedRecords(ctx, db, "partitioned_records_p0")
		require.NoError(t, err)
		require.Len(t, p0, 1)

		updated := []PartitionedRecord{{Id: id, State: 1, Msg: "after"}}
		require.NoError(t, UpsertPartitionedWithTransaction(
			ctx, db, partitionedTableName, []string{"id"}, updated))

		p0, err = selectPartitionedRecords(ctx, db, "partitioned_records_p0")
		require.NoError(t, err)
		assert.Empty(t, p0)

		p1, err := selectPartitionedRecords(ctx, db, "partitioned_records_p1")
		require.NoError(t, err)
		require.Len(t, p1, 1)
		assert.Equal(t, id, p1[0].Id)
		assert.Equal(t, 1, p1[0].State)
		assert.Equal(t, "after", p1[0].Msg)
		return nil
	})
	require.NoError(t, err)
}

func TestUpsertPartitioned_BatchMixed(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	err := withPartitionedDb(func(db *pgxpool.Pool) error {
		movingId := uuid.New()
		stableId := uuid.New()
		newId := uuid.New()

		initial := []PartitionedRecord{
			{Id: movingId, State: 0, Msg: "moving-before"},
			{Id: stableId, State: 1, Msg: "stable"},
		}
		require.NoError(t, UpsertPartitionedWithTransaction(
			ctx, db, partitionedTableName, []string{"id"}, initial))

		batch := []PartitionedRecord{
			{Id: movingId, State: 1, Msg: "moving-after"},
			{Id: newId, State: 0, Msg: "new"},
		}
		require.NoError(t, UpsertPartitionedWithTransaction(
			ctx, db, partitionedTableName, []string{"id"}, batch))

		all, err := selectPartitionedRecords(ctx, db, partitionedTableName)
		require.NoError(t, err)
		assert.Len(t, all, 3)

		byId := map[uuid.UUID]PartitionedRecord{}
		for _, r := range all {
			byId[r.Id] = r
		}
		assert.Equal(t, PartitionedRecord{Id: movingId, State: 1, Msg: "moving-after"}, byId[movingId])
		assert.Equal(t, PartitionedRecord{Id: stableId, State: 1, Msg: "stable"}, byId[stableId])
		assert.Equal(t, PartitionedRecord{Id: newId, State: 0, Msg: "new"}, byId[newId])
		return nil
	})
	require.NoError(t, err)
}

type partitionedRecordWithGenerated struct {
	Id       uuid.UUID `db:"id"`
	State    int       `db:"state"`
	Msg      string    `db:"msg"`
	MsgUpper string    `db:"msg_upper"`
}

const partitionedSchemaWithGenerated = `
CREATE TABLE partitioned_records (
    id        uuid NOT NULL,
    state     int  NOT NULL,
    msg       text NOT NULL,
    msg_upper text GENERATED ALWAYS AS (upper(msg)) STORED,
    PRIMARY KEY (id, state)
) PARTITION BY LIST (state);
CREATE TABLE partitioned_records_p0 PARTITION OF partitioned_records FOR VALUES IN (0);
CREATE TABLE partitioned_records_p1 PARTITION OF partitioned_records FOR VALUES IN (1);
`

func TestUpsertPartitioned_ExcludeColumns(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	err := WithTestDb([]Migration{NewMigration(1, "init", partitionedSchemaWithGenerated)},
		func(db *pgxpool.Pool) error {
			records := []partitionedRecordWithGenerated{
				{Id: uuid.New(), State: 0, Msg: "hello"},
			}
			require.NoError(t, UpsertPartitionedWithTransaction(
				ctx, db, partitionedTableName, []string{"id"}, records,
				WithExcludeColumns("msg_upper")))

			rows, err := db.Query(ctx,
				`SELECT msg, msg_upper FROM partitioned_records`)
			require.NoError(t, err)
			defer rows.Close()
			require.True(t, rows.Next())
			var msg, upper string
			require.NoError(t, rows.Scan(&msg, &upper))
			assert.Equal(t, "hello", msg)
			assert.Equal(t, "HELLO", upper)
			return nil
		})
	require.NoError(t, err)
}

func TestUpsertPartitioned_EmptyBatch(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	err := withPartitionedDb(func(db *pgxpool.Pool) error {
		err := UpsertPartitionedWithTransaction[PartitionedRecord](
			ctx, db, partitionedTableName, []string{"id"}, nil)
		assert.NoError(t, err)

		err = UpsertPartitionedWithTransaction(
			ctx, db, partitionedTableName, []string{"id"}, []PartitionedRecord{})
		assert.NoError(t, err)
		return nil
	})
	require.NoError(t, err)
}

func TestUpsertPartitioned_EmptyConflictKey(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	err := withPartitionedDb(func(db *pgxpool.Pool) error {
		records := []PartitionedRecord{{Id: uuid.New(), State: 0, Msg: "x"}}
		err := UpsertPartitionedWithTransaction(
			ctx, db, partitionedTableName, nil, records)
		assert.Error(t, err)
		return nil
	})
	require.NoError(t, err)
}

func TestUpsertPartitioned_ConflictKeyNotWritable(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	err := WithTestDb([]Migration{NewMigration(1, "init", partitionedSchemaWithGenerated)},
		func(db *pgxpool.Pool) error {
			records := []partitionedRecordWithGenerated{
				{Id: uuid.New(), State: 0, Msg: "hello"},
			}
			err := UpsertPartitionedWithTransaction(
				ctx, db, partitionedTableName, []string{"msg_upper"}, records,
				WithExcludeColumns("msg_upper"))
			assert.Error(t, err)

			err = UpsertPartitionedWithTransaction(
				ctx, db, partitionedTableName, []string{"nonexistent"}, records)
			assert.Error(t, err)
			return nil
		})
	require.NoError(t, err)
}
