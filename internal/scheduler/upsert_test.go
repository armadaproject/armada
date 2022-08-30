package scheduler

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/severinson/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/internal/scheduler/sql"
)

// Used for tests.
type Record struct {
	Id      uuid.UUID `db:"id"`
	Value   int       `db:"value"`
	Message string    `db:"message"`
	Notes   string    // Note no db tag
}

const SCHEMA string = `(
	id UUID PRIMARY KEY,
	value int NOT NULL,
	message text NOT NULL
)`

func (r Record) Schema() string {
	return SCHEMA
}

func TestNamesFromRecord(t *testing.T) {
	r := Record{
		Id:      uuid.New(),
		Value:   123,
		Message: "abcö",
	}
	names := NamesFromRecord(r)
	assert.Equal(t, []string{"id", "value", "message"}, names)
}

func TestValuesFromRecord(t *testing.T) {
	r := Record{
		Id:      uuid.New(),
		Value:   123,
		Message: "abcö",
	}
	values := ValuesFromRecord(r)
	assert.Equal(t, []interface{}{r.Id, r.Value, r.Message}, values)
}

func TestNamesValuesFromRecord(t *testing.T) {
	r := Record{
		Id:      uuid.New(),
		Value:   123,
		Message: "abcö",
	}
	names, values := NamesValuesFromRecord(r)
	assert.Equal(t, []string{"id", "value", "message"}, names)
	assert.Equal(t, []interface{}{r.Id, r.Value, r.Message}, values)
}

func TestNamesValuesFromRecordPointer(t *testing.T) {
	r := &Record{
		Id:      uuid.New(),
		Value:   123,
		Message: "abcö",
	}
	names, values := NamesValuesFromRecord(*r)
	assert.Equal(t, []string{"id", "value", "message"}, names)
	assert.Equal(t, []interface{}{r.Id, r.Value, r.Message}, values)
}

func withSetup(action func(queries *Queries, db *pgxpool.Pool, tableName string) error) error {
	ctx := context.Background()

	connectionString := "host=localhost port=5432 user=postgres password=psw sslmode=disable"
	db, err := pgxpool.Connect(ctx, connectionString)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Ping(ctx)
	if err != nil {
		return err
	}

	// Drop all existing tables.
	for _, table := range []string{"queues", "jobs", "runs", "job_errors", "job_run_errors", "pulsar", "nodeinfo"} {
		_, err = db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		if err != nil {
			return err
		}
	}

	// Setup fresh tables.
	_, err = db.Exec(ctx, sql.SchemaTemplate())
	if err != nil {
		return err
	}

	return action(New(db), db, "runs")
}

func TestUpsert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := withSetup(func(queries *Queries, db *pgxpool.Pool, tableName string) error {

		// Insert rows, read them back, and compare.
		expected := makeRuns(10)
		err := IdempotentUpsert(ctx, db, "topic", map[int32]pulsar.MessageID{0: pulsarutils.New(0, 0, 0, 0)}, tableName, RunsSchema(), interfacesFromRuns(expected))
		if !assert.NoError(t, err) {
			return nil
		}

		actual, err := queries.ListRuns(ctx)
		if !assert.NoError(t, err) {
			return nil
		}
		if !assertRunsEqual(t, expected, actual) {
			return nil
		}

		// Change one record, upsert, read back, and compare.
		expected[0].Executor = "foo"
		err = IdempotentUpsert(ctx, db, "topic", map[int32]pulsar.MessageID{0: pulsarutils.New(0, 1, 0, 0)}, tableName, RunsSchema(), interfacesFromRuns(expected))
		if !assert.NoError(t, err) {
			return nil
		}
		actual, err = queries.ListRuns(ctx)
		if !assert.NoError(t, err) {
			return nil
		}
		if !assertRunsEqual(t, expected, actual) {
			return nil
		}

		return nil
	})
	assert.NoError(t, err)
}

func TestIdempotence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := withSetup(func(queries *Queries, db *pgxpool.Pool, tableName string) error {

		// Insert rows, read them back, and compare.
		expected := makeRuns(10)
		records := expected
		writeMessageId := pulsarutils.New(0, 1, 0, 0)
		err := IdempotentUpsert(ctx, db, "topic", map[int32]pulsar.MessageID{0: writeMessageId}, tableName, RunsSchema(), interfacesFromRuns(records))
		if !assert.NoError(t, err) {
			return nil
		}

		actual, err := queries.ListRuns(ctx)
		if !assert.NoError(t, err) {
			return nil
		}
		if !assertRunsEqual(t, expected, actual) {
			return nil
		}

		// Insert with a lower id and check that it fails.
		dbMessageId := writeMessageId
		writeMessageId = pulsarutils.New(0, 0, 0, 0)
		records = makeRuns(1)
		err = IdempotentUpsert(ctx, db, "topic", map[int32]pulsar.MessageID{0: writeMessageId}, tableName, RunsSchema(), interfacesFromRuns(records))
		expectedErr := &ErrStaleWrite{
			Topic: "topic",
			StaleWrites: []StaleWrite{
				{
					DbMessageId:    dbMessageId,
					WriteMessageId: writeMessageId,
				},
			},
		}
		var e *ErrStaleWrite
		if assert.ErrorAs(t, err, &e) {
			if !assert.Equal(t, expectedErr, e) {
				return nil
			}

			ok, err := dbMessageId.Equal(e.StaleWrites[0].DbMessageId)
			if !assert.NoError(t, err) {
				return nil
			}
			if !assert.True(t, ok) {
				return nil
			}

			ok, err = writeMessageId.Equal(e.StaleWrites[0].WriteMessageId)
			if !assert.NoError(t, err) {
				return nil
			}
			if !assert.True(t, ok) {
				return nil
			}
		} else {
			return nil
		}

		actual, err = queries.ListRuns(ctx)
		if !assert.NoError(t, err) {
			return nil
		}
		if !assertRunsEqual(t, expected, actual) {
			return nil
		}

		return nil
	})
	assert.NoError(t, err)
}

func TestIdempotenceMultiPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := withSetup(func(queries *Queries, db *pgxpool.Pool, tableName string) error {

		// Insert rows, read them back, and compare.
		// Here, we emulate inserting a message based off of several partitions.
		expected := makeRuns(10)
		records := expected
		writeMessageIds := map[int32]pulsar.MessageID{
			0: pulsarutils.New(0, 1, 0, 0),
			1: pulsarutils.New(0, 2, 1, 0),
		}
		err := IdempotentUpsert(ctx, db, "topic", writeMessageIds, tableName, RunsSchema(), interfacesFromRuns(records))
		if !assert.NoError(t, err) {
			return nil
		}

		actual, err := queries.ListRuns(ctx)
		if !assert.NoError(t, err) {
			return nil
		}
		if !assertRunsEqual(t, expected, actual) {
			return nil
		}

		// Insert with a lower id for one of the partitions and check that it fails.
		dbMessageIds := writeMessageIds
		writeMessageIds = map[int32]pulsar.MessageID{
			0: pulsarutils.New(0, 1, 0, 0),
			1: pulsarutils.New(0, 1, 1, 0),
		}
		records = makeRuns(1)
		err = IdempotentUpsert(ctx, db, "topic", writeMessageIds, tableName, RunsSchema(), interfacesFromRuns(records))
		expectedErr := &ErrStaleWrite{
			Topic: "topic",
			StaleWrites: []StaleWrite{ // Stale write on the 1-th partition.
				{
					DbMessageId:    dbMessageIds[1],
					WriteMessageId: writeMessageIds[1],
				},
			},
		}
		var e *ErrStaleWrite
		if assert.ErrorAs(t, err, &e) {
			if !assert.Equal(t, expectedErr, e) {
				return nil
			}

			dbMessageId, ok := dbMessageIds[1].(*pulsarutils.PulsarMessageId)
			if !assert.True(t, ok) {
				return nil
			}
			writeMessageId, ok := writeMessageIds[1].(*pulsarutils.PulsarMessageId)
			if !assert.True(t, ok) {
				return nil
			}

			ok, err := dbMessageId.Equal(e.StaleWrites[0].DbMessageId)
			if !assert.NoError(t, err) {
				return nil
			}
			if !assert.True(t, ok) {
				return nil
			}

			ok, err = writeMessageId.Equal(e.StaleWrites[0].WriteMessageId)
			if !assert.NoError(t, err) {
				return nil
			}
			if !assert.True(t, ok) {
				return nil
			}
		} else {
			return nil
		}

		actual, err = queries.ListRuns(ctx)
		if !assert.NoError(t, err) {
			return nil
		}
		if !assertRunsEqual(t, expected, actual) {
			return nil
		}

		return nil
	})
	assert.NoError(t, err)
}

func TestConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := withSetup(func(queries *Queries, db *pgxpool.Pool, tableName string) error {

		// Each thread inserts non-overlapping rows, reads them back, and compares.
		for i := 0; i < 100; i++ {
			i := i
			expected := makeRuns(10)
			executor := fmt.Sprintf("executor-%d", i)
			setRunsExecutor(expected, executor)
			err := IdempotentUpsert(ctx, db, fmt.Sprintf("topic-%d", i), map[int32]pulsar.MessageID{0: pulsarutils.New(0, 0, 0, 0)}, tableName, RunsSchema(), interfacesFromRuns(expected))
			if !assert.NoError(t, err) {
				return nil
			}

			runs, err := queries.ListRuns(ctx)
			if !assert.NoError(t, err) {
				return nil
			}
			actual := make([]Run, 0)
			for _, run := range runs {
				if run.Executor == executor {
					actual = append(actual, run)
				}
			}
			if !assertRunsEqual(t, expected, actual) {
				return nil
			}
		}

		return nil
	})
	assert.NoError(t, err)
}

// Return nrecords records, each with payloadSize bytes of payload.
// The returned slice is sorted by ID.
func makeRecords(nrecords, payloadSize int) []interface{} {
	records := make([]Record, nrecords)
	payload := strings.Repeat("0", payloadSize)
	for i := 0; i < nrecords; i++ {
		records[i] = Record{
			Id:      uuid.New(),
			Value:   i,
			Message: payload,
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].Id.String() < records[j].Id.String()
	})
	rv := make([]interface{}, nrecords)
	for i, v := range records {
		rv[i] = v
	}
	return rv
}

// assertRunsEqual is a utility function for comparing two slices of runs.
// First sorts both slices.
func assertRunsEqual(t *testing.T, expected, actual []Run) bool {
	sort.Slice(expected, func(i, j int) bool {
		return expected[i].RunID.String() < expected[j].RunID.String()
	})
	sort.Slice(actual, func(i, j int) bool {
		return actual[i].RunID.String() < actual[j].RunID.String()
	})
	return assert.Equal(t, expected, actual)
}

// makeRuns is a utility functions that returns n randomly generated runs structs.
func makeRuns(n int) []Run {
	runs := make([]Run, n)
	for i := 0; i < n; i++ {
		runs[i] = Run{
			RunID:        uuid.New(),
			JobID:        uuid.New(),
			Executor:     uuid.NewString(), // A randomly generated string used for tests.
			LastModified: time.Date(2022, time.July, 13, 9, 27, 0, 0, time.Local),
		}
	}
	return runs
}

func setRunsExecutor(runs []Run, executor string) {
	for i := range runs {
		runs[i].Executor = executor
	}
}

func interfacesFromRuns(runs []Run) []interface{} {
	rv := make([]interface{}, len(runs))
	for i, v := range runs {
		rv[i] = v
	}
	return rv
}

func TestAutoIncrement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := withSetup(func(queries *Queries, db *pgxpool.Pool, tableName string) error {

		// Insert two rows. These should automatically get auto-incrementing serial numbers
		// 1 and 2, respectively.
		var records []interface{}
		records = append(
			records,
			Nodeinfo{
				NodeName: "foo",
				Message:  make([]byte, 0),
			},
			Nodeinfo{
				NodeName: "bar",
				Message:  []byte{1},
			},
		)
		err := Upsert(ctx, db, "nodeinfo", NodeInfoSchema(), records)
		if !assert.NoError(t, err) {
			return nil
		}

		actual, err := queries.ListNodeInfo(ctx)
		if !assert.NoError(t, err) {
			return nil
		}
		assert.Equal(t, 2, len(actual))
		assert.Equal(t, int64(1), actual[0].Serial)
		assert.Equal(t, int64(2), actual[1].Serial)
		assert.Equal(t, "foo", actual[0].NodeName)
		assert.Equal(t, "bar", actual[1].NodeName)

		// Update one of the records.
		// Should automatically set the serial of the row to 3.
		records = make([]interface{}, 0)
		records = append(records, Nodeinfo{
			NodeName: "bar",
			Message:  []byte{2},
		})

		err = Upsert(ctx, db, "nodeinfo", NodeInfoSchema(), records)
		if !assert.NoError(t, err) {
			return nil
		}

		actual, err = queries.ListNodeInfo(ctx)
		if !assert.NoError(t, err) {
			return nil
		}
		assert.Equal(t, 2, len(actual))
		assert.Equal(t, int64(1), actual[0].Serial)
		assert.Equal(t, int64(3), actual[1].Serial)
		assert.Equal(t, "foo", actual[0].NodeName)
		assert.Equal(t, "bar", actual[1].NodeName)

		// Update one of the records.
		// Should automatically set the serial of the row to 3.
		records = make([]interface{}, 0)
		records = append(records, Nodeinfo{
			NodeName: "baz",
			Message:  []byte{2},
		})

		err = Upsert(ctx, db, "nodeinfo", NodeInfoSchema(), records)
		if !assert.NoError(t, err) {
			return nil
		}

		actual, err = queries.ListNodeInfo(ctx)
		if !assert.NoError(t, err) {
			return nil
		}
		assert.Equal(t, 3, len(actual))
		assert.Equal(t, int64(4), actual[2].Serial)
		assert.Equal(t, "baz", actual[2].NodeName)

		return nil
	})
	assert.NoError(t, err)
}

// Plain:
// upserted 10000 records in 150.714434ms

// Both
// upserted 10000 records in 314.750961ms

// Only serial
// upserted 10000 records in 309.336981ms

// Only timestamp
// upserted 10000 records in 203.027894ms

// Accessing the series directly
// upserted 10000 records in 210.879139ms

// Getting table name and schema programatically
// upserted 10000 records in 241.215724ms

func TestFoo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := withSetup(func(queries *Queries, db *pgxpool.Pool, tableName string) error {
		var records []interface{}
		for i := 0; i < 10000; i++ {
			records = append(
				records,
				Nodeinfo{
					NodeName: uuid.NewString(),
					Message:  make([]byte, 0),
				},
			)
		}

		start := time.Now()
		err := Upsert(ctx, db, "nodeinfo", NodeInfoSchema(), records)
		if !assert.NoError(t, err) {
			return nil
		}
		elapsed := time.Since(start)

		fmt.Printf("upserted %d records in %s (%d records per second)\n", len(records), elapsed, int64(elapsed)/int64(len(records))/1e9)
		// fmt.Println("upserted in ", time.Since(start), " seconds")

		return nil
	})
	assert.NoError(t, err)
}
