package lookout

import (
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/lookoutv2/schema"
)

func WithLookoutDb(action func(db *pgxpool.Pool) error) error {
	migrations, err := schema.LookoutMigrations()
	if err != nil {
		return err
	}
	return database.WithTestDb(migrations, action)
}
