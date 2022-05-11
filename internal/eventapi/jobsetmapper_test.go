package eventapi

import (
	"context"
	"github.com/G-Research/armada/internal/common/database"
	"github.com/G-Research/armada/internal/eventapi/eventdb"
	"github.com/G-Research/armada/internal/eventapi/eventdb/schema/statik"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var baseTime, _ = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")

func TestHappyPath(t *testing.T) {
	err := WithDatabase(func(db *eventdb.EventDb) error {
		ctx := context.Background()
		jobsetMapper, err := NewJobsetMapper(db, 100, 1*time.Second)
		assert.NoError(t, err)

		id1, err := jobsetMapper.Get(ctx, "fish", "chips")
		assert.NoError(t, err)
		id2, err := jobsetMapper.Get(ctx, "fish", "vinegar")
		assert.NoError(t, err)
		id3, err := jobsetMapper.Get(ctx, "fish", "chips")
		assert.NoError(t, err)
		id4, err := jobsetMapper.Get(ctx, "fish", "vinegar")
		assert.NoError(t, err)

		assert.NotEqual(t, id1, id2)
		assert.Equal(t, id1, id3)
		assert.Equal(t, id2, id4)
		return nil
	})
	assert.NoError(t, err)
}

func WithDatabase(action func(db *eventdb.EventDb) error) error {
	migrations, err := database.GetMigrations(statik.EventapiSql)
	if err != nil {
		return err
	}
	return database.WithTestDb(migrations, func(db *pgxpool.Pool) error {
		return action(eventdb.NewEventDb(db))
	})
}
