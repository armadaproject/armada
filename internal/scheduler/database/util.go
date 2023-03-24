package database

import (
	"context"
	"embed"
	_ "embed"
	"time"

	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/database"
)

//go:embed migrations/*.sql
var fs embed.FS

// Migrate updates the supplied database to the latest version.
// If the database is already at the latest version ten this is a no-op
func Migrate(ctx context.Context, db pgxtype.Querier) error {
	start := time.Now()
	migrations, err := database.ReadMigrations(fs, "migrations")
	if err != nil {
		return err
	}
	err = database.UpdateDatabase(ctx, db, migrations)
	if err != nil {
		return err
	}
	log.Infof("Updated scheduler database in %s", time.Now().Sub(start))
	return nil
}

// WithTestDb creates a scheduler database suitable for testing.  This will instantiate a completely
// new Postgres database which will be torn down after this function completes
func WithTestDb(action func(queries *Queries, db *pgxpool.Pool) error) error {
	migrations, err := database.ReadMigrations(fs, "migrations")
	if err != nil {
		return err
	}
	return database.WithTestDb(migrations, func(db *pgxpool.Pool) error {
		return action(New(db), db)
	})
}
