package database

import (
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/G-Research/armada/internal/common/database"
)

func WithSchedulerDb(action func(queries *Queries, db *pgxpool.Pool) error) error {
	// TODO: make the scheduler database properly support migrations
	migrations := []database.Migration{
		database.NewMigration(1, "initial", SchemaTemplate()),
	}
	return database.WithTestDb(migrations, func(db *pgxpool.Pool) error {
		return action(sqlc.New(db), db)
	})
}
