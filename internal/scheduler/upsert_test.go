package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"

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

func withSetup(action func(queries *Queries, db *pgxpool.Pool) error) error {
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
	for _, table := range []string{"queues", "jobs", "runs", "job_run_assignments", "job_errors", "job_run_errors", "pulsar", "nodeinfo", "leaderelection"} {
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

	return action(New(db), db)
}

func TestUpsert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := withSetup(func(queries *Queries, db *pgxpool.Pool) error {
		// Insert rows, read them back, and compare.
		expected := makeRecords(10)
		err := Upsert(ctx, db, "nodeinfo", NodeInfoSchema(), interfacesFromSlice(expected))
		if !assert.NoError(t, err) {
			return nil
		}

		actual, err := queries.SelectNewNodeInfo(ctx, 0)
		if !assert.NoError(t, err) {
			return nil
		}
		if !assertNodeInfoEqual(t, expected, actual) {
			return nil
		}

		// Change one record, upsert, read back, and compare.
		expected[0].Executor = "foo"
		err = Upsert(ctx, db, "nodeinfo", NodeInfoSchema(), interfacesFromSlice(expected))
		if !assert.NoError(t, err) {
			return nil
		}
		actual, err = queries.SelectNewNodeInfo(ctx, 0)
		if !assert.NoError(t, err) {
			return nil
		}
		if !assertNodeInfoEqual(t, expected, actual) {
			return nil
		}

		return nil
	})
	assert.NoError(t, err)
}

func TestConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := withSetup(func(queries *Queries, db *pgxpool.Pool) error {
		// Each thread inserts non-overlapping rows, reads them back, and compares.
		for i := 0; i < 100; i++ {
			i := i
			expected := makeRecords(10)
			executor := fmt.Sprintf("executor-%d", i)
			setExecutor(expected, executor)
			err := Upsert(ctx, db, "nodeinfo", NodeInfoSchema(), interfacesFromSlice(expected))
			if !assert.NoError(t, err) {
				return nil
			}

			vs, err := queries.SelectNewNodeInfo(ctx, 0)
			if !assert.NoError(t, err) {
				return nil
			}
			actual := make([]Nodeinfo, 0)
			for _, v := range vs {
				if v.Executor == executor {
					actual = append(actual, v)
				}
			}
			if !assertNodeInfoEqual(t, expected, actual) {
				return nil
			}
		}

		return nil
	})
	assert.NoError(t, err)
}

func assertNodeInfoEqual(t *testing.T, expected, actual []Nodeinfo) bool {
	es := make(map[string]*Nodeinfo)
	as := make(map[string]*Nodeinfo)
	for _, nodeinfo := range expected {
		v := &nodeinfo
		v.Serial = 0
		v.LastModified = time.Time{}
		es[v.ExecutorNodeName] = v
	}
	for _, nodeinfo := range actual {
		v := &nodeinfo
		v.Serial = 0
		v.LastModified = time.Time{}
		as[v.ExecutorNodeName] = v
	}
	return assert.Equal(t, es, as)
}

// makeRecords is a utility functions that returns n randomly generated records for insertion.
func makeRecords(n int) []Nodeinfo {
	vs := make([]Nodeinfo, n)
	for i := 0; i < n; i++ {
		vs[i] = Nodeinfo{
			ExecutorNodeName: uuid.NewString(),
			NodeName:         uuid.NewString(),
			Executor:         uuid.NewString(),
			Message:          make([]byte, 0),
		}
	}
	return vs
}

func setExecutor(runs []Nodeinfo, executor string) {
	for i := range runs {
		runs[i].Executor = executor
	}
}

func interfacesFromSlice[T any](vs []T) []interface{} {
	rv := make([]interface{}, len(vs))
	for i, v := range vs {
		rv[i] = v
	}
	return rv
}

func TestAutoIncrement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := withSetup(func(queries *Queries, db *pgxpool.Pool) error {
		// Insert two rows. These should automatically get auto-incrementing serial numbers.
		var records []interface{}
		records = append(
			records,
			Nodeinfo{
				ExecutorNodeName: "Efoo",
				NodeName:         "foo",
				Executor:         "E",
				Message:          make([]byte, 0),
			},
			Nodeinfo{
				ExecutorNodeName: "Ebar",
				NodeName:         "bar",
				Executor:         "E",
				Message:          []byte{1},
			},
		)
		err := Upsert(ctx, db, "nodeinfo", NodeInfoSchema(), records)
		if !assert.NoError(t, err) {
			return nil
		}

		actual, err := queries.SelectNewNodeInfo(ctx, 0)
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
			ExecutorNodeName: "Ebar",
			NodeName:         "bar",
			Executor:         "E",
			Message:          []byte{1},
		})

		err = Upsert(ctx, db, "nodeinfo", NodeInfoSchema(), records)
		if !assert.NoError(t, err) {
			return nil
		}

		actual, err = queries.SelectNewNodeInfo(ctx, 0)
		if !assert.NoError(t, err) {
			return nil
		}
		assert.Equal(t, 2, len(actual))
		assert.Equal(t, int64(1), actual[0].Serial)
		assert.Equal(t, int64(4), actual[1].Serial)
		assert.Equal(t, "foo", actual[0].NodeName)
		assert.Equal(t, "bar", actual[1].NodeName)

		// Update one of the records.
		// Should automatically set the serial of the row to 3.
		records = make([]interface{}, 0)
		records = append(records, Nodeinfo{
			ExecutorNodeName: "Ebaz",
			NodeName:         "baz",
			Executor:         "E",
			Message:          []byte{2},
		})

		err = Upsert(ctx, db, "nodeinfo", NodeInfoSchema(), records)
		if !assert.NoError(t, err) {
			return nil
		}

		actual, err = queries.SelectNewNodeInfo(ctx, 0)
		if !assert.NoError(t, err) {
			return nil
		}
		assert.Equal(t, 3, len(actual))
		assert.Equal(t, int64(5), actual[2].Serial)
		assert.Equal(t, "baz", actual[2].NodeName)

		return nil
	})
	assert.NoError(t, err)
}
