package database

import (
	"embed"
	_ "embed"
	"github.com/armadaproject/armada/internal/common/context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/database"
)

//go:embed migrations/*.sql
var fs embed.FS

func Migrate(ctx *context.ArmadaContext, db database.Querier) error {
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

func WithTestDb(action func(queries *Queries, db *pgxpool.Pool) error) error {
	migrations, err := database.ReadMigrations(fs, "migrations")
	if err != nil {
		return err
	}
	return database.WithTestDb(migrations, func(db *pgxpool.Pool) error {
		return action(New(db), db)
	})
}
