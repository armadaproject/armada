package database

import (
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

func TestReadMigrations(t *testing.T) {
	fsys := fstest.MapFS{
		"migrations/README.md":                        {Data: []byte("ignored")},
		"migrations/010_after_two_digit_ids.sql":      {Data: []byte("after")},
		"migrations/001_a.sql":                        {Data: []byte("a")},
		"migrations/005_unknown_suffix.up.sql":        {Data: []byte("up")},
		"migrations/002_b.sql":                        {Data: []byte("b")},
		"migrations/002_b.partitioned.sql":            {Data: []byte("b-partitioned")},
		"migrations/003_c.partitioned/02_second.sql":  {Data: []byte("c-2")},
		"migrations/003_c.partitioned/01_first.sql":   {Data: []byte("c-1")},
		"migrations/003_c.partitioned/README.md":      {Data: []byte("ignored")},
		"migrations/004_d/01_only.sql":                {Data: []byte("d")},
		"migrations/004_d.partitioned/01_variant.sql": {Data: []byte("d-partitioned")},
		"migrations/004_d.partitioned/nested/x.sql":   {Data: []byte("ignored")},
	}

	migrations, err := ReadMigrations(fsys, "migrations", "partitioned")
	require.NoError(t, err)

	assert.Equal(t, []Migration{
		{id: 1, name: "001_a", variants: map[string][]string{"": {"a"}}},
		{id: 2, name: "002_b", variants: map[string][]string{"": {"b"}, "partitioned": {"b-partitioned"}}},
		{id: 3, name: "003_c", variants: map[string][]string{"partitioned": {"c-1", "c-2"}}},
		{id: 4, name: "004_d", variants: map[string][]string{"": {"d"}, "partitioned": {"d-partitioned"}}},
		{id: 5, name: "005_unknown_suffix.up", variants: map[string][]string{"": {"up"}}},
		{id: 10, name: "010_after_two_digit_ids", variants: map[string][]string{"": {"after"}}},
	}, migrations)
}

func TestReadMigrations_Errors(t *testing.T) {
	tests := map[string]fstest.MapFS{
		"variant defined twice": {
			"migrations/001_a.partitioned.sql":        {Data: []byte("a")},
			"migrations/001_a.partitioned/01_one.sql": {Data: []byte("a")},
		},
		"directory without sql files": {
			"migrations/001_a.partitioned/README.md": {Data: []byte("a")},
		},
		"non-numeric id": {
			"migrations/abc_a.sql": {Data: []byte("a")},
		},
	}
	for name, fsys := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := ReadMigrations(fsys, "migrations", "partitioned")
			assert.Error(t, err)
		})
	}
}

func TestUpdateDatabaseWithVariants(t *testing.T) {
	fsys := fstest.MapFS{
		"migrations/001_create.sql":                          {Data: []byte("CREATE TABLE applied (step text)")},
		"migrations/002_both.sql":                            {Data: []byte("INSERT INTO applied VALUES ('002 default')")},
		"migrations/002_both.partitioned.sql":                {Data: []byte("INSERT INTO applied VALUES ('002 partitioned')")},
		"migrations/003_variant_only.partitioned/01_row.sql": {Data: []byte("INSERT INTO applied VALUES ('003 partitioned')")},
		// CREATE INDEX CONCURRENTLY fails inside a transaction, so this
		// proves that each statement in a directory is executed separately.
		"migrations/003_variant_only.partitioned/02_index.sql": {Data: []byte("CREATE INDEX CONCURRENTLY idx_applied_step ON applied (step)")},
		"migrations/004_default_only.sql":                      {Data: []byte("INSERT INTO applied VALUES ('004 default')")},
	}
	migrations, err := ReadMigrations(fsys, "migrations", "partitioned")
	require.NoError(t, err)

	tests := map[string]struct {
		selectVariant VariantSelector
		expectedSteps []string
		expectIndex   bool
	}{
		"nil selector applies default variants": {
			selectVariant: nil,
			expectedSteps: []string{"002 default", "004 default"},
		},
		"selected variant overrides default and falls back where missing": {
			selectVariant: func(*armadacontext.Context, Querier) (string, error) { return "partitioned", nil },
			expectedSteps: []string{"002 partitioned", "003 partitioned", "004 default"},
			expectIndex:   true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := WithTestDb(nil, func(db *pgxpool.Pool) error {
				ctx := armadacontext.Background()
				require.NoError(t, UpdateDatabaseWithVariants(ctx, db, migrations, tc.selectVariant))

				version, err := readVersion(ctx, db)
				require.NoError(t, err)
				assert.Equal(t, 4, version)

				rows, err := db.Query(ctx, `SELECT step FROM applied ORDER BY step`)
				require.NoError(t, err)
				var steps []string
				for rows.Next() {
					var step string
					require.NoError(t, rows.Scan(&step))
					steps = append(steps, step)
				}
				require.NoError(t, rows.Err())
				assert.Equal(t, tc.expectedSteps, steps)

				var indexExists bool
				require.NoError(t, db.QueryRow(ctx,
					`SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_applied_step')`,
				).Scan(&indexExists))
				assert.Equal(t, tc.expectIndex, indexExists)
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestUpdateDatabaseWithVariants_SelectsVariantBeforeEachPendingMigration(t *testing.T) {
	migrations := []Migration{
		NewMigration(1, "001_create", "CREATE TABLE applied (step text)"),
		{id: 2, name: "002_insert", variants: map[string][]string{
			"":       {"INSERT INTO applied VALUES ('002 default')"},
			"second": {"INSERT INTO applied VALUES ('002 second')"},
		}},
		{id: 3, name: "003_insert", variants: map[string][]string{
			"":       {"INSERT INTO applied VALUES ('003 default')"},
			"second": {"INSERT INTO applied VALUES ('003 second')"},
		}},
	}
	calls := 0
	selectSecondFromThirdCall := func(*armadacontext.Context, Querier) (string, error) {
		calls++
		if calls >= 3 {
			return "second", nil
		}
		return "", nil
	}

	err := WithTestDb(nil, func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()
		require.NoError(t, UpdateDatabaseWithVariants(ctx, db, migrations, selectSecondFromThirdCall))
		assert.Equal(t, 3, calls)

		require.NoError(t, UpdateDatabaseWithVariants(ctx, db, migrations, selectSecondFromThirdCall))
		assert.Equal(t, 3, calls, "selector should not be called for applied migrations")

		var steps []string
		rows, err := db.Query(ctx, `SELECT step FROM applied ORDER BY step`)
		require.NoError(t, err)
		for rows.Next() {
			var step string
			require.NoError(t, rows.Scan(&step))
			steps = append(steps, step)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"002 default", "003 second"}, steps)
		return nil
	})
	require.NoError(t, err)
}
