package schema

import (
	"embed"

	"github.com/armadaproject/armada/internal/common/database"
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
