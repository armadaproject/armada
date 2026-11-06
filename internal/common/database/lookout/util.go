package lookout

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	lookoutschema "github.com/armadaproject/armada/internal/lookout/schema"
)

// WithLookoutDb spins up a fresh Lookout test database with the lookout
// migration chain applied and the job table partitioned, as lookout
// --migrateDatabase leaves a new install, and runs action against it.
func WithLookoutDb(action func(db *pgxpool.Pool) error) error {
	lookoutMigrations, err := lookoutschema.LookoutMigrations()
	if err != nil {
		return err
	}

	return database.WithTestDb(lookoutMigrations, func(db *pgxpool.Pool) error {
		if err := lookoutschema.PartitionJobTable(armadacontext.Background(), db); err != nil {
			return err
		}
		return action(db)
	})
}
