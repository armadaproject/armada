package lookout

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	lookoutschema "github.com/armadaproject/armada/internal/lookout/schema"
	lookouthcschema "github.com/armadaproject/armada/internal/lookouthc/schema"
)

// WithLookoutDb spins up two Lookout test databases in sequence and runs
// action against each:
//
//  1. a fresh database with the lookout migration chain applied;
//  2. a fresh database with the lookout migration chain applied, then
//     converted to the partitioned hot-cold shape via ApplyPartitioner.
//
// This exercises shared test logic against both the non-partitioned and
// the experimental partitioned schema.
func WithLookoutDb(action func(db *pgxpool.Pool) error) error {
	lookoutMigrations, err := lookoutschema.LookoutMigrations()
	if err != nil {
		return err
	}

	if err := database.WithTestDb(lookoutMigrations, action); err != nil {
		return err
	}

	return database.WithTestDb(lookoutMigrations, func(db *pgxpool.Pool) error {
		if err := lookouthcschema.ApplyPartitioner(armadacontext.Background(), db); err != nil {
			return err
		}
		return action(db)
	})
}
