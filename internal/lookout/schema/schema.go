package schema

import (
	"embed"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
)

// PartitionedVariant is the name of the migration variant applied when the
// job table is partitioned.
const PartitionedVariant = "partitioned"

//go:embed migrations
var fs embed.FS

func LookoutMigrations() ([]database.Migration, error) {
	migrations, err := database.ReadMigrations(fs, "migrations", PartitionedVariant)
	if err != nil {
		return nil, err
	}
	return migrations, nil
}

// UpdateDatabase applies migrations to db, selecting each migration's
// PartitionedVariant if the job table is partitioned when it runs.
func UpdateDatabase(ctx *armadacontext.Context, db database.Querier, migrations []database.Migration) error {
	return database.UpdateDatabaseWithVariants(ctx, db, migrations, selectJobTableVariant)
}

func selectJobTableVariant(ctx *armadacontext.Context, db database.Querier) (string, error) {
	partitioned, err := IsJobTablePartitioned(ctx, db)
	if err != nil {
		return "", err
	}
	if partitioned {
		return PartitionedVariant, nil
	}
	return "", nil
}
