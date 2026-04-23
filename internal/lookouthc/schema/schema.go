package schema

import (
	"embed"

	"github.com/armadaproject/armada/internal/common/database"
)

//go:embed migrations/*.sql
var fs embed.FS

// LookoutHCMigrations returns the migrations for the lookouthc (hot-cold)
// database schema. These are a consolidated set capturing the final state of
// the original Lookout migrations, with the job table natively partitioned by
// state.
func LookoutHCMigrations() ([]database.Migration, error) {
	migrations, err := database.ReadMigrations(fs, "migrations")
	if err != nil {
		return nil, err
	}
	return migrations, nil
}
