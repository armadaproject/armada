package database

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/server/configuration"
)

const testConnectionString = "host=localhost port=5432 user=postgres password=psw sslmode=disable"

// WithTestDb spins up a Postgres database for testing
// migrations: perform the list of migrations before entering the action callback
// action: callback for client code
//
// The full migration chain is applied once to a reusable template database per
// migration set, and each call clones that template with
// "CREATE DATABASE ... TEMPLATE" - a filesystem-level copy far cheaper than
// re-running every migration. Without this, running many Postgres-backed tests
// in parallel (as mage tests does) re-migrates a fresh database for every test,
// overwhelming the shared test Postgres and causing context deadline exceeded
// flakes.
func WithTestDb(migrations []Migration, action func(db *pgxpool.Pool) error) error {
	ctx := armadacontext.Background()

	admin, err := pgx.Connect(ctx, testConnectionString)
	if err != nil {
		return errors.WithStack(err)
	}
	defer admin.Close(ctx)

	templateName, err := ensureTemplate(ctx, admin, migrations)
	if err != nil {
		return err
	}

	// Clone the migrated template into a dedicated database for the test.
	dbName := "test_" + util.NewULID()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+dbName+" TEMPLATE "+templateName); err != nil {
		return errors.WithStack(err)
	}

	// Disconnect all db users and drop the database we created at test completion.
	defer func() {
		if _, err := admin.Exec(ctx,
			`SELECT pg_terminate_backend(pg_stat_activity.pid)
			 FROM pg_stat_activity WHERE pg_stat_activity.datname = '`+dbName+`';`,
		); err != nil {
			fmt.Println("failed to disconnect users:", err)
		}
		if _, err := admin.Exec(ctx, "DROP DATABASE "+dbName); err != nil {
			fmt.Println("failed to drop database:", err)
		}
	}()

	// Connect again: this time to the database we just created. This is the database we use for tests.
	testDbPool, err := pgxpool.New(ctx, testConnectionString+" dbname="+dbName)
	if err != nil {
		return errors.WithStack(err)
	}

	return action(testDbPool)
}

// processId distinguishes templates created by different test binaries running
// in parallel, so each process migrates its own template instead of racing on a
// shared name.
var processId = util.NewULID()

var (
	templateMu sync.Mutex
	templates  = map[string]string{} // migration-set key -> template database name
)

// ensureTemplate returns a database that has migrations applied. The template is
// created once per (process, migration set) and reused for all subsequent calls.
func ensureTemplate(ctx *armadacontext.Context, admin *pgx.Conn, migrations []Migration) (string, error) {
	key := migrationKey(migrations)

	templateMu.Lock()
	defer templateMu.Unlock()

	if name, ok := templates[key]; ok {
		return name, nil
	}

	name := "test_template_" + processId + "_" + key
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		return "", errors.WithStack(err)
	}

	templatePool, err := pgxpool.New(ctx, testConnectionString+" dbname="+name)
	if err != nil {
		return "", errors.WithStack(err)
	}
	if err := UpdateDatabase(ctx, templatePool, migrations); err != nil {
		templatePool.Close()
		return "", errors.WithStack(err)
	}
	templatePool.Close()

	templates[key] = name
	return name, nil
}

// migrationKey derives a stable identifier from the migration chain so that
// different migration sets (e.g. lookout vs scheduler) get separate templates.
func migrationKey(migrations []Migration) string {
	h := sha256.New()
	for _, m := range migrations {
		fmt.Fprintf(h, "%d:%s:%s;", m.id, m.name, m.sql)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// WithTestDbCustom connects to specified database for testing
// migrations: perform the list of migrations before entering the action callback
// config: PostgresConfig to specify connection details to database
// action: callback for client code
func WithTestDbCustom(migrations []Migration, config configuration.PostgresConfig, action func(db *pgxpool.Pool) error) error {
	ctx := armadacontext.Background()

	testDbPool, err := OpenPgxPool(config)
	if err != nil {
		return errors.WithStack(err)
	}
	defer testDbPool.Close()

	err = UpdateDatabase(ctx, testDbPool, migrations)
	if err != nil {
		return errors.WithStack(err)
	}

	return action(testDbPool)
}
