package lookout

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/armadaproject/armada/internal/common/database"
	lookoutschema "github.com/armadaproject/armada/internal/lookout/schema"
)

// WithLookoutDb spins up a Lookout test database with the full migration
// chain applied and runs action against it.
func WithLookoutDb(action func(db *pgxpool.Pool) error) error {
	lookoutMigrations, err := lookoutschema.LookoutMigrations()
	if err != nil {
		return err
	}
	return database.WithTestDb(lookoutMigrations, action)
}
