package lookout

import (
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/G-Research/armada/internal/common/database"
	"github.com/G-Research/armada/internal/lookoutv2/schema/statik"
)

func WithLookoutDb(action func(db *pgxpool.Pool) error) error {
	migrations, err := database.GetMigrations(statik.Lookoutv2Sql)
	if err != nil {
		return err
	}
	return database.WithTestDb(migrations, action)
}
