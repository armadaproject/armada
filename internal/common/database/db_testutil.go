package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"

	"github.com/G-Research/armada/internal/common/util"
)

func WithTestDb(migrations []migration, action func(db *pgxpool.Pool) error) error {
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
	testDbPool, err := pgxpool.Connect(ctx, connectionString+" dbname="+dbName)
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
