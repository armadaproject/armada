package database

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/server/configuration"
)

func TestPrepareSchema_ValidatesConfig(t *testing.T) {
	ctx := armadacontext.Background()

	t.Run("CreateSchema without Schema is rejected", func(t *testing.T) {
		err := PrepareSchema(ctx, nil, MigrationConfig{CreateSchema: true})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CreateSchema requires Schema")
	})

	t.Run("zero value is a no-op", func(t *testing.T) {
		err := PrepareSchema(ctx, nil, MigrationConfig{})
		require.NoError(t, err)
	})

	t.Run("rejects empty identifier", func(t *testing.T) {
		assert.Error(t, validateIdentifier(""))
	})

	t.Run("rejects over-length schema name", func(t *testing.T) {
		err := PrepareSchema(ctx, nil, MigrationConfig{Schema: strings.Repeat("a", maxIdentifierLength+1)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds maximum length")
	})

	t.Run("accepts valid identifiers up to length cap", func(t *testing.T) {
		assert.NoError(t, validateIdentifier("armada_test"))
		assert.NoError(t, validateIdentifier("My-Schema"))
		assert.NoError(t, validateIdentifier(strings.Repeat("a", maxIdentifierLength)))
	})
}

func TestPrepareSchema_SingleRole_CreatesAndSetsSearchPath(t *testing.T) {
	err := WithTestDb(nil, func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()
		conn := acquireConn(t, ctx, db)
		defer conn.Close(ctx)
		cfg := MigrationConfig{Schema: "armada_test", CreateSchema: true}

		require.NoError(t, PrepareSchema(ctx, conn, cfg))
		assertSchemaExists(t, db, "armada_test")
		assertSearchPath(t, conn, "armada_test")

		// Idempotent.
		require.NoError(t, PrepareSchema(ctx, conn, cfg))
		assertSchemaExists(t, db, "armada_test")
		return nil
	})
	require.NoError(t, err)
}

func TestPrepareSchema_SchemaSetButNotCreated(t *testing.T) {
	err := WithTestDb(nil, func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()
		conn := acquireConn(t, ctx, db)
		defer conn.Close(ctx)
		_, err := conn.Exec(ctx, `CREATE SCHEMA armada_test`)
		require.NoError(t, err)

		require.NoError(t, PrepareSchema(ctx, conn, MigrationConfig{Schema: "armada_test"}))
		assertSearchPath(t, conn, "armada_test")
		return nil
	})
	require.NoError(t, err)
}

func TestPrepareSchema_QuotesNamesNeedingQuoting(t *testing.T) {
	err := WithTestDb(nil, func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()
		conn := acquireConn(t, ctx, db)
		defer conn.Close(ctx)
		schema := "Armada-Mixed"
		cfg := MigrationConfig{Schema: schema, CreateSchema: true}

		require.NoError(t, PrepareSchema(ctx, conn, cfg))
		assertSchemaExists(t, db, schema)
		// Idempotent for quoted names.
		require.NoError(t, PrepareSchema(ctx, conn, cfg))
		return nil
	})
	require.NoError(t, err)
}

func TestPrepareSchema_NoSchema_LeavesSearchPathAlone(t *testing.T) {
	err := WithTestDb(nil, func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()
		conn := acquireConn(t, ctx, db)
		defer conn.Close(ctx)

		var before string
		require.NoError(t, conn.QueryRow(ctx, `SHOW search_path`).Scan(&before))

		require.NoError(t, PrepareSchema(ctx, conn, MigrationConfig{}))

		var after string
		require.NoError(t, conn.QueryRow(ctx, `SHOW search_path`).Scan(&after))
		assert.Equal(t, before, after)
		return nil
	})
	require.NoError(t, err)
}

func TestPrepareSchema_TwoRole_CreatesAndGrants(t *testing.T) {
	err := WithTestDb(nil, func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()

		// Create a least-privileged migrator role on the test database.
		const migratorRole = "armada_migrator_test"
		const migratorPassword = "psw"
		dbName := databaseName(t, ctx, db)
		migratorRoleIdent := pgx.Identifier{migratorRole}.Sanitize()

		_, err := db.Exec(ctx, fmt.Sprintf(`DROP ROLE IF EXISTS %s`, migratorRoleIdent))
		require.NoError(t, err)
		_, err = db.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, migratorRoleIdent, migratorPassword))
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanupCtx := armadacontext.Background()
			_, _ = db.Exec(cleanupCtx, fmt.Sprintf(`REVOKE ALL ON SCHEMA armada_test FROM %s`, migratorRoleIdent))
			_, _ = db.Exec(cleanupCtx, fmt.Sprintf(`DROP ROLE IF EXISTS %s`, migratorRoleIdent))
		})

		// Open a connection as the migrator role.
		migratorCfg := configuration.PostgresConfig{Connection: map[string]string{
			"host":     "localhost",
			"port":     "5432",
			"user":     migratorRole,
			"password": migratorPassword,
			"dbname":   dbName,
			"sslmode":  "disable",
		}}
		migratorConn, err := OpenPgxConn(migratorCfg)
		require.NoError(t, err)
		defer migratorConn.Close(ctx)

		// Sanity check: the migrator role cannot create the schema itself.
		_, err = migratorConn.Exec(ctx, `CREATE SCHEMA armada_test`)
		require.Error(t, err)

		// Bootstrap config points at the privileged "postgres" superuser
		// (the same account WithTestDb itself uses).
		bootstrapCfg := configuration.PostgresConfig{Connection: map[string]string{
			"host":     "localhost",
			"port":     "5432",
			"user":     "postgres",
			"password": "psw",
			"dbname":   dbName,
			"sslmode":  "disable",
		}}

		cfg := MigrationConfig{
			Schema:                      "armada_test",
			CreateSchema:                true,
			SchemaCreatorPostgresConfig: &bootstrapCfg,
		}
		require.NoError(t, PrepareSchema(ctx, migratorConn, cfg))

		assertSchemaExists(t, db, "armada_test")
		assertSearchPath(t, migratorConn, "armada_test")

		// Migrator role can now create objects in the schema.
		_, err = migratorConn.Exec(ctx, `CREATE TABLE t (id int)`)
		require.NoError(t, err)

		// Idempotent.
		require.NoError(t, PrepareSchema(ctx, migratorConn, cfg))
		return nil
	})
	require.NoError(t, err)
}

func acquireConn(t *testing.T, ctx *armadacontext.Context, pool *pgxpool.Pool) *pgx.Conn {
	t.Helper()
	acquired, err := pool.Acquire(ctx)
	require.NoError(t, err)
	conn := acquired.Hijack()
	return conn
}

func assertSchemaExists(t *testing.T, db Querier, schema string) {
	t.Helper()
	var exists bool
	err := db.QueryRow(armadacontext.Background(),
		`SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`, schema,
	).Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists, "schema %s should exist", schema)
}

func assertSearchPath(t *testing.T, db Querier, expected string) {
	t.Helper()
	var got string
	err := db.QueryRow(armadacontext.Background(), `SHOW search_path`).Scan(&got)
	require.NoError(t, err)
	assert.Contains(t, got, expected)
}

func databaseName(t *testing.T, ctx *armadacontext.Context, db Querier) string {
	t.Helper()
	var name string
	err := db.QueryRow(ctx, `SELECT current_database()`).Scan(&name)
	require.NoError(t, err)
	return name
}
