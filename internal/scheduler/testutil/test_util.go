package testutil

import (
	"github.com/G-Research/armada/internal/common/database"
	"github.com/G-Research/armada/internal/scheduler"
	"github.com/G-Research/armada/internal/scheduler/sql"
	"github.com/jackc/pgx/v4/pgxpool"
)

func WithSchedulerDb(action func(queries *scheduler.Queries, db *pgxpool.Pool) error) error {
	//TODO: make the scheduler database properly support migrations
	migrations := []database.Migration{
		database.NewMigration(1, "initial", sql.SchemaTemplate()),
	}
	return database.WithTestDb(migrations, func(db *pgxpool.Pool) error {
		return action(scheduler.New(db), db)
	})
}
