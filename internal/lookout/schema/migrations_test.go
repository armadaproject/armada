package schema

import (
	"fmt"
	"regexp"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
)

// firstPartitionedMigrationID is the latest migration at the time the
// experimental partitioner first shipped. Databases may have been
// partitioned at any version since, then had the rest of the chain applied.
const firstPartitionedMigrationID = 32

// TestChainThenConvertMatchesConvertThenRestOfChain verifies that the
// conversion and the migration chain stay in sync: a database partitioned at
// any point since firstPartitionedMigrationID, then migrated to the latest
// version, must match one migrated to the latest version and then
// partitioned.
func TestChainThenConvertMatchesConvertThenRestOfChain(t *testing.T) {
	ctx := armadacontext.Background()
	migrations, err := LookoutMigrations()
	require.NoError(t, err)

	var expected []string
	err = database.WithTestDb(nil, func(db *pgxpool.Pool) error {
		require.NoError(t, UpdateDatabase(ctx, db, migrations))
		require.NoError(t, PartitionJobTable(ctx, db))
		expected = catalogue(t, db)
		return nil
	})
	require.NoError(t, err)

	for i, m := range migrations {
		if m.ID() < firstPartitionedMigrationID {
			continue
		}
		t.Run(fmt.Sprintf("partitioned after %s", m.Name()), func(t *testing.T) {
			err := database.WithTestDb(nil, func(db *pgxpool.Pool) error {
				require.NoError(t, UpdateDatabase(ctx, db, migrations[:i+1]))
				require.NoError(t, PartitionJobTable(ctx, db))
				require.NoError(t, UpdateDatabase(ctx, db, migrations))

				shape, detail, err := DetectJobTableShape(ctx, db)
				require.NoError(t, err)
				assert.Equal(t, JobTableShapePartitioned, shape, detail)
				actual := catalogue(t, db)
				assert.Empty(t, linesNotIn(expected, actual), "only after chain, then convert")
				assert.Empty(t, linesNotIn(actual, expected), "only after convert, then rest of chain")
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestPartitionJobTable_RefusesToDropColumns(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()
		require.NoError(t, insertUnpartitionedJob(ctx, db, "jobA", 1))
		_, err := db.Exec(ctx, `ALTER TABLE job ADD COLUMN not_in_target text`)
		require.NoError(t, err)

		err = PartitionJobTable(ctx, db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not_in_target")

		partitioned, err := IsJobTablePartitioned(ctx, db)
		require.NoError(t, err)
		assert.False(t, partitioned, "a failed conversion must leave job unpartitioned")
		var n int
		require.NoError(t, db.QueryRow(ctx, `SELECT count(*) FROM job`).Scan(&n))
		assert.Equal(t, 1, n)
		return nil
	})
	require.NoError(t, err)
}

// partitionedIndexMigration is the reference pattern for adding an index to
// job without blocking writes on either shape. CREATE INDEX CONCURRENTLY is
// not allowed on a partitioned table, so the partitioned variant creates an
// invalid parent index on job only, builds each partition's index
// concurrently, then attaches them, which makes the parent index valid. A
// failed concurrent build leaves an INVALID index that IF NOT EXISTS would
// skip, so any invalid partition index is dropped before building.
var partitionedIndexMigration = fstest.MapFS{
	"migrations/1000_create_job_cancel_user_idx.sql": {Data: []byte(
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_cancel_user ON job (cancel_user)`)},
	"migrations/1000_create_job_cancel_user_idx.partitioned/01_parent.sql": {Data: []byte(
		`CREATE INDEX IF NOT EXISTS idx_job_cancel_user ON ONLY job (cancel_user)`)},
	"migrations/1000_create_job_cancel_user_idx.partitioned/02_drop_invalid.sql": {Data: []byte(`
DO $$
DECLARE
    idx text;
BEGIN
    FOR idx IN
        SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE c.relname IN ('idx_job_active_cancel_user', 'idx_job_terminated_cancel_user')
        AND c.relnamespace = current_schema()::regnamespace AND NOT i.indisvalid
    LOOP
        EXECUTE format('DROP INDEX %I', idx);
    END LOOP;
END $$`)},
	"migrations/1000_create_job_cancel_user_idx.partitioned/03_active.sql": {Data: []byte(
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_active_cancel_user ON job_active (cancel_user)`)},
	"migrations/1000_create_job_cancel_user_idx.partitioned/04_terminated.sql": {Data: []byte(
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_terminated_cancel_user ON job_terminated (cancel_user)`)},
	"migrations/1000_create_job_cancel_user_idx.partitioned/05_attach_active.sql": {Data: []byte(
		`ALTER INDEX idx_job_cancel_user ATTACH PARTITION idx_job_active_cancel_user`)},
	"migrations/1000_create_job_cancel_user_idx.partitioned/06_attach_terminated.sql": {Data: []byte(
		`ALTER INDEX idx_job_cancel_user ATTACH PARTITION idx_job_terminated_cancel_user`)},
}

func TestPartitionedIndexMigrationPattern(t *testing.T) {
	ctx := armadacontext.Background()
	lookoutMigrations, err := LookoutMigrations()
	require.NoError(t, err)
	indexMigration, err := database.ReadMigrations(partitionedIndexMigration, "migrations", PartitionedVariant)
	require.NoError(t, err)
	migrations := append(lookoutMigrations, indexMigration...)

	t.Run("unpartitioned", func(t *testing.T) {
		err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
			require.NoError(t, UpdateDatabase(ctx, db, migrations))
			assert.True(t, indexIsValid(t, db, "idx_job_cancel_user"))
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("partitioned, recovering from a failed build and re-run", func(t *testing.T) {
		err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
			require.NoError(t, PartitionJobTable(ctx, db))

			// A failed concurrent build leaves an INVALID index behind.
			require.NoError(t, insertUnpartitionedJob(ctx, db, "jobA", 4))
			require.NoError(t, insertUnpartitionedJob(ctx, db, "jobB", 4))
			_, err := db.Exec(ctx, `UPDATE job SET cancel_user = 'same'`)
			require.NoError(t, err)
			_, err = db.Exec(ctx, `CREATE UNIQUE INDEX CONCURRENTLY idx_job_terminated_cancel_user ON job_terminated (cancel_user)`)
			require.Error(t, err)
			require.False(t, indexIsValid(t, db, "idx_job_terminated_cancel_user"))

			require.NoError(t, UpdateDatabase(ctx, db, migrations))
			assertPartitionedIndexComplete(t, db)

			// Recording a migration is not atomic with applying it, so a
			// crash in between re-runs it.
			_, err = db.Exec(ctx, `SELECT setval('database_version', $1)`, indexMigration[0].ID()-1)
			require.NoError(t, err)
			require.NoError(t, UpdateDatabase(ctx, db, migrations))
			assertPartitionedIndexComplete(t, db)
			return nil
		})
		require.NoError(t, err)
	})
}

func assertPartitionedIndexComplete(t *testing.T, db *pgxpool.Pool) {
	t.Helper()
	for _, idx := range []string{"idx_job_cancel_user", "idx_job_active_cancel_user", "idx_job_terminated_cancel_user"} {
		assert.True(t, indexIsValid(t, db, idx), "%s should be valid", idx)
	}
	var unique bool
	require.NoError(t, db.QueryRow(armadacontext.Background(), `
		SELECT i.indisunique FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		WHERE c.relname = 'idx_job_terminated_cancel_user'`).Scan(&unique))
	assert.False(t, unique, "the invalid index should have been rebuilt")
}

func indexIsValid(t *testing.T, db *pgxpool.Pool, name string) bool {
	t.Helper()
	var valid bool
	require.NoError(t, db.QueryRow(armadacontext.Background(), `
		SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		WHERE c.relname = $1 AND c.relnamespace = current_schema()::regnamespace`, name).Scan(&valid))
	return valid
}

var indexNameRegex = regexp.MustCompile(`(CREATE (?:UNIQUE )?INDEX) \S+ ON`)

// catalogue describes the schema of db as sorted, comparable lines: the
// version, tables with their options and partition bounds, columns, indexes
// and constraints. Indexes and constraints that Postgres creates on a
// partition to match its parent are named after the parent, because their
// own names depend on the table name when they were created.
func catalogue(t *testing.T, db *pgxpool.Pool) []string {
	t.Helper()
	ctx := armadacontext.Background()
	queries := []string{
		`SELECT 'version ' || last_value FROM database_version`,
		`SELECT format('table %s kind=%s options=%s bound=%s', c.relname, c.relkind, c.reloptions, pg_get_expr(c.relpartbound, c.oid))
		 FROM pg_class c
		 WHERE c.relnamespace = current_schema()::regnamespace AND c.relkind IN ('r', 'p')`,
		`SELECT format('column %s.%s #%s %s notnull=%s default=%s storage=%s',
		         c.relname, a.attname, a.attnum, format_type(a.atttypid, a.atttypmod), a.attnotnull,
		         pg_get_expr(d.adbin, d.adrelid), a.attstorage)
		 FROM pg_attribute a
		 JOIN pg_class c ON c.oid = a.attrelid
		 LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		 WHERE c.relnamespace = current_schema()::regnamespace AND c.relkind IN ('r', 'p')
		 AND a.attnum > 0 AND NOT a.attisdropped`,
		`SELECT format('index %s %s valid=%s', coalesce('child of ' || parent.relname, c.relname), pg_get_indexdef(c.oid), i.indisvalid)
		 FROM pg_index i
		 JOIN pg_class c ON c.oid = i.indexrelid
		 LEFT JOIN pg_inherits inh ON inh.inhrelid = c.oid
		 LEFT JOIN pg_class parent ON parent.oid = inh.inhparent
		 WHERE c.relnamespace = current_schema()::regnamespace`,
		`SELECT format('constraint %s on %s %s', coalesce('child of ' || parent.conname, con.conname), c.relname, pg_get_constraintdef(con.oid))
		 FROM pg_constraint con
		 JOIN pg_class c ON c.oid = con.conrelid
		 LEFT JOIN pg_constraint parent ON parent.oid = con.conparentid
		 WHERE c.relnamespace = current_schema()::regnamespace`,
	}

	var lines []string
	for _, query := range queries {
		rows, err := db.Query(ctx, query)
		require.NoError(t, err)
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			lines = append(lines, line)
		}
		require.NoError(t, rows.Err())
	}
	for i, line := range lines {
		lines[i] = indexNameRegex.ReplaceAllString(line, "$1 ON")
	}
	slices.Sort(lines)
	return lines
}

func linesNotIn(lines []string, other []string) []string {
	var result []string
	for _, line := range lines {
		if !slices.Contains(other, line) {
			result = append(result, line)
		}
	}
	return result
}
