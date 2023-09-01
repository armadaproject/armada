package testutil

import (
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/context"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookout/repository/schema"
)

func WithDatabase(action func(db *sql.DB) error) error {
	dbName := "test_" + util.NewULID()
	connectionString := "host=localhost port=5432 user=postgres password=psw sslmode=disable"
	db, err := sql.Open("pgx", connectionString)
	if err != nil {
		return errors.WithStack(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE " + dbName)
	if err != nil {
		return errors.WithStack(err)
	}

	testDb, err := sql.Open("pgx", connectionString+" dbname="+dbName)
	if err != nil {
		return errors.WithStack(err)
	}

	defer func() {
		err = testDb.Close()
		if err != nil {
			fmt.Println("Failed to close testDb")
		}

		// disconnect all db user before cleanup
		_, err = db.Exec(
			`SELECT pg_terminate_backend(pg_stat_activity.pid)
			 FROM pg_stat_activity WHERE pg_stat_activity.datname = '` + dbName + `';`)
		if err != nil {
			fmt.Println("Failed to disconnect users")
		}

		_, err = db.Exec("DROP DATABASE " + dbName)
		if err != nil {
			fmt.Println("Failed to drop database")
		}
	}()

	err = schema.UpdateDatabase(testDb)
	if err != nil {
		return errors.WithStack(err)
	}

	return action(testDb)
}

func WithDatabasePgx(action func(db *pgxpool.Pool) error) error {
	ctx := context.Background()

	// Connect and create a dedicated database for the test
	// For now use database/sql for this
	dbName := "test_" + util.NewULID()
	connectionString := "host=localhost port=5432 user=postgres password=psw sslmode=disable"
	db, err := sql.Open("pgx", connectionString)
	if err != nil {
		return errors.WithStack(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE " + dbName)
	if err != nil {
		return errors.WithStack(err)
	}

	// Connect again- this time to the database we just created and using pgx pool.  This will be used for tests
	testDbPool, err := pgxpool.New(ctx, connectionString+" dbname="+dbName)
	if err != nil {
		return errors.WithStack(err)
	}

	defer func() {
		testDbPool.Close()

		// disconnect all db user before cleanup
		_, err = db.Exec(
			`SELECT pg_terminate_backend(pg_stat_activity.pid)
			 FROM pg_stat_activity WHERE pg_stat_activity.datname = '` + dbName + `';`)
		if err != nil {
			fmt.Println("Failed to disconnect users")
		}

		_, err = db.Exec("DROP DATABASE " + dbName)
		if err != nil {
			fmt.Println("Failed to drop database")
		}
	}()

	// A third connection!  We can get rid of this once we use move udateDatabse over to pgx
	legacyDb, err := sql.Open("pgx", connectionString+" dbname="+dbName)
	if err != nil {
		return errors.WithStack(err)
	}
	defer legacyDb.Close()

	err = schema.UpdateDatabase(legacyDb)
	if err != nil {
		return errors.WithStack(err)
	}

	return action(testDbPool)
}
