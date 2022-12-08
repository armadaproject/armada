package schema

import (
	"context"
	"embed"

	"github.com/jackc/pgtype/pgxtype"

	"github.com/G-Research/armada/internal/common/database"
)

//go:embed migrations/*.sql
var fs embed.FS

func LookoutMigrations() ([]database.Migration, error) {
	migrations, err := database.ReadMigrations(fs, "migrations")
	if err != nil {
		return nil, err
	}
	return migrations, nil
}

func MigrateLookout(ctx context.Context, db pgxtype.Querier) error {
	migrations, err := database.ReadMigrations(fs, "migrations")
	if err != nil {
		return err
	}
	return database.UpdateDatabase(ctx, db, migrations)
}
