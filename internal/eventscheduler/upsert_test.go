package state

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
)

const schemaFile = "./sql/schema.sql"

func withSetup(action func(queries *Queries, db *pgx.Conn) error) error {
	ctx := context.Background()
	db, err := pgx.Connect(ctx, "postgres://postgres:password@localhost:5432/test")
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

	_, err = db.Exec(ctx, "DROP TABLE IF EXISTS pulsar")
	if err != nil {
		return err
	}

	dat, err := os.ReadFile(schemaFile)
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx, string(dat))
	if err != nil {
		return err
	}

	queries := New(db)

	return action(queries, db)
}

func TestUpsertMany(t *testing.T) {
	ctx := context.Background()
	err := withSetup(func(queries *Queries, db *pgx.Conn) error {

		// Test inserts
		records := makeRecords(10, 10)
		start := time.Now()
		writeMessageId := pulsar.EarliestMessageID()
		err := UpsertRecords(ctx, db, writeMessageId, records)
		assert.NoError(t, err)
		fmt.Printf("upserted %d records in %s\n", len(records), time.Since(start))

		fetchedRecords, err := queries.ListRecords(ctx)
		assert.NoError(t, err)
		assert.Equal(t, records, fetchedRecords)

		// Test updates
		incrementValues(records)
		writeMessageId = pulsar.LatestMessageID()
		start = time.Now()
		err = UpsertRecords(ctx, db, writeMessageId, records)
		assert.NoError(t, err)
		fmt.Printf("updated %d records in %s\n", len(records), time.Since(start))

		fetchedRecords, err = queries.ListRecords(ctx)
		assert.NoError(t, err)
		assert.Equal(t, records, fetchedRecords)

		return nil
	})
	assert.NoError(t, err)
}

// Return nrecords records, each with payloadSize bytes of payload.
// valueOffset is added to the value embedded in each record. Useful for testing db updates.
// The returned slice is sorted by ID.
func makeRecords(nrecords, payloadSize int) []Record {
	records := make([]Record, nrecords)
	payload := strings.Repeat("0", payloadSize)
	for i := 0; i < nrecords; i++ {
		records[i] = Record{
			ID:      uuid.New(),
			Value:   int32(i),
			Payload: payload,
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].ID.String() < records[j].ID.String()
	})
	return records
}

// Increment the values stored in records. Useful to test updates.
func incrementValues(records []Record) {
	for _, record := range records {
		record.Value += 1
	}
}
