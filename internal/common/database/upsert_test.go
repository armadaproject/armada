package database

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

// Used for tests.
type Record struct {
	Id      uuid.UUID `db:"id"`
	Value   int       `db:"value"`
	Message string    `db:"message"`
	Serial  int64     `db:"serial"`
}

const SCHEMA string = `(
	id UUID PRIMARY KEY,
	value int NOT NULL,
	message text NOT NULL,
    serial bigserial NOT NULL
)`

const TABLE_NAME = "records"

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
	assert.Equal(t, []string{"id", "value", "message", "serial"}, names)
}

func TestValuesFromRecord(t *testing.T) {
	r := Record{
		Id:      uuid.New(),
		Value:   123,
		Message: "abcö",
		Serial:  0,
	}
	values := ValuesFromRecord(r)
	assert.Equal(t, []interface{}{r.Id, r.Value, r.Message, r.Serial}, values)
}

func TestNamesValuesFromRecord(t *testing.T) {
	r := Record{
		Id:      uuid.New(),
		Value:   123,
		Message: "abcö",
	}
	names, values := NamesValuesFromRecord(r)
	assert.Equal(t, []string{"id", "value", "message", "serial"}, names)
	assert.Equal(t, []interface{}{r.Id, r.Value, r.Message, r.Serial}, values)
}

func TestNamesValuesFromRecordPointer(t *testing.T) {
	r := &Record{
		Id:      uuid.New(),
		Value:   123,
		Message: "abcö",
	}
	names, values := NamesValuesFromRecord(*r)
	assert.Equal(t, []string{"id", "value", "message", "serial"}, names)
	assert.Equal(t, []interface{}{r.Id, r.Value, r.Message, r.Serial}, values)
}

func TestUpsert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Hour)
	defer cancel()
	err := withDb(func(db *pgxpool.Pool) error {
		// Insert rows, read them back, and compare.
		expected := makeRecords(10)
		err := Upsert(ctx, db, TABLE_NAME, SCHEMA, interfacesFromSlice(expected))
		if !assert.NoError(t, err) {
			return nil
		}

		actual, err := selectRecords(db)
		if !assert.NoError(t, err) {
			return nil
		}
		if !assertRecordsEqual(t, expected, actual) {
			return nil
		}

		// Change one record, upsert, read back, and compare.
		expected[0].Message = "foo"
		err = Upsert(ctx, db, TABLE_NAME, SCHEMA, interfacesFromSlice(expected))
		if !assert.NoError(t, err) {
			return nil
		}
		actual, err = selectRecords(db)
		if !assert.NoError(t, err) {
			return nil
		}
		assertRecordsEqual(t, expected, actual)
		return nil
	})
	assert.NoError(t, err)
}

func TestConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := withDb(func(db *pgxpool.Pool) error {
		// Each thread inserts non-overlapping rows, reads them back, and compares.
		for i := 0; i < 100; i++ {
			i := i
			expected := makeRecords(10)
			executor := fmt.Sprintf("executor-%d", i)
			setMessageToExecutor(expected, executor)
			err := Upsert(ctx, db, TABLE_NAME, SCHEMA, interfacesFromSlice(expected))
			if !assert.NoError(t, err) {
				return nil
			}

			vs, err := selectRecords(db)
			if !assert.NoError(t, err) {
				return nil
			}
			actual := make([]Record, 0)
			for _, v := range vs {
				if v.Message == executor {
					actual = append(actual, v)
				}
			}
			if !assertRecordsEqual(t, expected, actual) {
				return nil
			}
		}

		return nil
	})
	assert.NoError(t, err)
}

func TestAutoIncrement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := withDb(func(db *pgxpool.Pool) error {
		// Insert two rows. These should automatically get auto-incrementing serial numbers.
		records := makeRecords(2)
		err := Upsert(ctx, db, TABLE_NAME, SCHEMA, interfacesFromSlice(records))
		if !assert.NoError(t, err) {
			return nil
		}

		actual, err := selectRecords(db)
		if !assert.NoError(t, err) {
			return nil
		}
		assert.Equal(t, 2, len(actual))
		assert.Equal(t, int64(1), actual[0].Serial)
		assert.Equal(t, int64(2), actual[1].Serial)

		// Update one of the records.
		// Should automatically set the serial of the row to 3.
		records[1].Message = "foo"

		err = Upsert(ctx, db, TABLE_NAME, SCHEMA, interfacesFromSlice(records))
		if !assert.NoError(t, err) {
			return nil
		}

		actual, err = selectRecords(db)
		if !assert.NoError(t, err) {
			return nil
		}
		assert.Equal(t, 2, len(actual))
		assert.Equal(t, int64(1), actual[0].Serial)
		assert.Equal(t, int64(4), actual[1].Serial)
		assert.Equal(t, records[1].Message, actual[1].Message)

		return nil
	})
	assert.NoError(t, err)
}

func assertRecordsEqual(t *testing.T, expected, actual []Record) bool {
	assert.Equal(t, len(expected), len(actual), "records not of equal length")
	es := make(map[string]*Record)
	as := make(map[string]*Record)
	for _, record := range expected {
		v := &record
		v.Serial = 0
		es[v.Id.String()] = v
	}
	for _, record := range actual {
		v := &record
		v.Serial = 0
		as[v.Id.String()] = v
	}
	return assert.Equal(t, es, as)
}

// makeRecords is a utility functions that returns n randomly generated records for insertion.
func makeRecords(n int) []Record {
	vs := make([]Record, n)
	for i := 0; i < n; i++ {
		vs[i] = Record{
			Id:      uuid.New(),
			Value:   rand.Intn(1000000),
			Message: uuid.NewString(),
		}
	}
	return vs
}

func setMessageToExecutor(runs []Record, executor string) {
	for i := range runs {
		runs[i].Message = executor
	}
}

func interfacesFromSlice[T any](vs []T) []interface{} {
	rv := make([]interface{}, len(vs))
	for i, v := range vs {
		rv[i] = v
	}
	return rv
}

func selectRecords(db *pgxpool.Pool) ([]Record, error) {
	rows, err := db.Query(context.Background(), fmt.Sprintf("SELECT * FROM %s", TABLE_NAME))
	if err != nil {
		return nil, err
	}
	records := make([]Record, 0)
	for rows.Next() {
		record := Record{}
		rows.Scan(&record)
		records = append(records, record)
	}
	return records, nil
}

func withDb(action func(db *pgxpool.Pool) error) error {
	var ddl = fmt.Sprintf("CREATE TABLE %s %s", TABLE_NAME, SCHEMA)
	println(ddl)
	return WithTestDb([]Migration{NewMigration(1, "init", ddl)}, action)
}
