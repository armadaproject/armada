package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/util"
)

// WithTestDb spins up a Postgres database for testing
// migrations: perform the list of migrations before entering the action callback
// action: callback for client code
func WithTestDb(migrations []Migration, action func(db *pgxpool.Pool) error) error {
	ctx := context.Background()

	// Connect and create a dedicated database for the test
	dbName := "test_" + util.NewULID()
	connectionString := "host=localhost port=5432 user=postgres password=psw sslmode=disable"
	db, err := pgx.Connect(ctx, connectionString)
	if err != nil {
		return errors.WithStack(err)
	}
	defer db.Close(ctx)

	_, err = db.Exec(ctx, "CREATE DATABASE "+dbName)
	if err != nil {
		return errors.WithStack(err)
	}

	// Connect again: this time to the database we just created.  This is the databse we use for tests
	testDbPool, err := pgxpool.New(ctx, connectionString+" dbname="+dbName)
	if err != nil {
		return errors.WithStack(err)
	}

	defer func() {
		// disconnect all db user before cleanup
		_, err = db.Exec(ctx,
			`SELECT pg_terminate_backend(pg_stat_activity.pid)
			 FROM pg_stat_activity WHERE pg_stat_activity.datname = '`+dbName+`';`)
		if err != nil {
			fmt.Println("Failed to disconnect users")
		}

		_, err = db.Exec(ctx, "DROP DATABASE "+dbName)
		if err != nil {
			fmt.Println("Failed to drop database")
		}
	}()

	err = UpdateDatabase(ctx, testDbPool, migrations)
	if err != nil {
		return errors.WithStack(err)
	}

	return action(testDbPool)
}

// WithTestDbCustom connects to specified database for testing
// migrations: perform the list of migrations before entering the action callback
// config: PostgresConfig to specify connection details to database
// action: callback for client code
func WithTestDbCustom(migrations []Migration, config configuration.PostgresConfig, action func(db *pgxpool.Pool) error) error {
	ctx := context.Background()

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
