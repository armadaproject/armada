package lookout

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/armadaproject/armada/internal/common/database"
	lookoutschema "github.com/armadaproject/armada/internal/lookout/schema"
	lookouthcschema "github.com/armadaproject/armada/internal/lookouthc/schema"
)

func WithLookoutDb(action func(db *pgxpool.Pool) error) error {
	lookoutMigrations, err := lookoutschema.LookoutMigrations()
	if err != nil {
		return err
	}
	if err := database.WithTestDb(lookoutMigrations, action); err != nil {
		return err
	}

	lookoutHCMigrations, err := lookouthcschema.LookoutHCMigrations()
	if err != nil {
		return err
	}
	return database.WithTestDb(lookoutHCMigrations, action)
}
