package database

import (
	"context"
	"embed"
	_ "embed"

	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/armadaproject/armada/internal/common/database"
)

//go:embed migrations/*.sql
var fs embed.FS

func Migrate(ctx context.Context, db pgxtype.Querier) error {
	migrations, err := database.ReadMigrations(fs, "migrations")
	if err != nil {
		return err
	}
	return database.UpdateDatabase(ctx, db, migrations)
}

func WithTestDb(action func(queries *Queries, db *pgxpool.Pool) error) error {
	migrations, err := database.ReadMigrations(fs, "migrations")
	if err != nil {
		return err
	}
	return database.WithTestDb(migrations, func(db *pgxpool.Pool) error {
		return action(New(db), db)
	})
}
