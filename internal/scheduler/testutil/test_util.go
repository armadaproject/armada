package testutil

import (
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/G-Research/armada/internal/common/database"
	"github.com/G-Research/armada/internal/scheduler/sql"
	"github.com/G-Research/armada/internal/scheduler/sqlc"
)

func WithSchedulerDb(action func(queries *sqlc.Queries, db *pgxpool.Pool) error) error {
	// TODO: make the scheduler database properly support migrations
	migrations := []database.Migration{
		database.NewMigration(1, "initial", sql.SchemaTemplate()),
	}
	return database.WithTestDb(migrations, func(db *pgxpool.Pool) error {
		return action(sqlc.New(db), db)
	})
}
