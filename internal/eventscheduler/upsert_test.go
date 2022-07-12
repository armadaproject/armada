package eventscheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
)

// Record type used to test upserts.
type Record struct {
	Id      uuid.UUID `db:"id"`
	Value   int       `db:"value"`
	Message string    `db:"message"`
}

// func (r Record) Names() []string {
// 	return []string{"id", "value", "message"}
// }

// func (r Record) Values() ([]interface{}, error) {
// 	return []interface{}{r.id, r.value, r.message}, nil
// }

const SCHEMA string = `(
	id UUID PRIMARY KEY,
	value int NOT NULL,
	message text NOT NULL
)`

func (r Record) Schema() string {
	return SCHEMA
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

func withSetup(action func(queries *Queries, db *pgx.Conn, tableName string) error) error {
	ctx := context.Background()

	connectionString := "host=localhost port=5432 user=postgres password=psw sslmode=disable"
	db, err := pgx.Connect(ctx, connectionString)
	// db, err := pgx.Connect(ctx, "postgres://postgres:pws@localhost:5432/test")
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	err = db.Ping(ctx)
	if err != nil {
		return err
	}

	_, err = db.Exec(ctx, "DROP TABLE IF EXISTS records")
	if err != nil {
		return err
	}

	_, err = db.Exec(ctx, "DROP TABLE IF EXISTS runs")
	if err != nil {
		return err
	}

	_, err = db.Exec(ctx, "DROP TABLE IF EXISTS pulsar")
	if err != nil {
		return err
	}

	_, err = db.Exec(ctx, fmt.Sprintf("CREATE TABLE records %s;", SCHEMA))
	if err != nil {
		return err
	}

	fmt.Println(RunsSchema)
	_, err = db.Exec(ctx, fmt.Sprintf("CREATE TABLE runs %s;", RunsSchema))
	if err != nil {
		return err
	}

	_, err = db.Exec(ctx, fmt.Sprintf("CREATE TABLE pulsar %s;", PulsarSchema))
	if err != nil {
		return err
	}

	return action(New(db), db, "runs")
}

func TestUpsertMany(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := withSetup(func(queries *Queries, db *pgx.Conn, tableName string) error {
		// var records []UpsertRecord
		records := makeRuns(10)
		// records[0].Schema()
		start := time.Now()
		writeMessageId := pulsarutils.New(0, 0, 0, 0)
		fmt.Println("tableName ", tableName)
		err := UpsertRecords(ctx, db, pulsarutils.FromMessageId(writeMessageId), tableName, "topic", RunsSchema, records)
		// err := UpsertRecords(ctx, db, writeMessageId, records)
		if !assert.NoError(t, err) {
			return nil
		}
		fmt.Printf("upserted %d records in %s\n", len(records), time.Since(start))

		fetchedRuns, err := queries.ListRuns(ctx)
		if !assert.NoError(t, err) {
			return nil
		}
		expected := make([]Run, len(records))
		for i, v := range records {
			foo, ok := v.(Run)
			if !ok {
				return errors.New("could not convert")
			}
			expected[i] = foo
			fetchedRuns[i].LastModified = time.Time{}
		}
		assert.Equal(t, expected, fetchedRuns)

		// // Test updates
		// incrementValues(records)
		// writeMessageId = pulsar.LatestMessageID()
		// start = time.Now()
		// err = UpsertRecords(ctx, db, writeMessageId, records)
		// assert.NoError(t, err)
		// fmt.Printf("updated %d records in %s\n", len(records), time.Since(start))

		// fetchedRecords, err = queries.ListRecords(ctx)
		// assert.NoError(t, err)
		// assert.Equal(t, records, fetchedRecords)

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
		fmt.Println("rv[i] ", i, " ", v)
	}
	return rv
}

// // Increment the values stored in records. Useful to test updates.
// func incrementValues(records []Record) {
// 	for _, record := range records {
// 		record.Value += 1
// 	}
// }

// Return nrecords records, each with payloadSize bytes of payload.
// The returned slice is sorted by ID.
func makeRuns(nrecords int) []interface{} {
	vs := make([]Run, nrecords)
	for i := 0; i < nrecords; i++ {
		vs[i] = Run{
			RunID:      uuid.New(),
			JobID:      uuid.New(),
			Executor:   "executor",
			Assignment: pgtype.JSON{},
		}
		vs[i].Assignment.Set(struct {
			Node string `json:"node"`
		}{Node: "foo"})
	}
	sort.Slice(vs, func(i, j int) bool {
		return vs[i].RunID.String() < vs[j].RunID.String()
	})
	rv := make([]interface{}, nrecords)
	for i, v := range vs {
		rv[i] = v
		fmt.Println("rv[i] ", i, " ", v)
	}
	return rv
}
