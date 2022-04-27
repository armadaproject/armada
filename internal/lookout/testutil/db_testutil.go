package testutil

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/lookout/repository/schema"
)

func WithDatabase(t *testing.T, action func(db *sql.DB)) {
	dbName := "test_" + util.NewULID()
	connectionString := "host=localhost port=5432 user=postgres password=psw sslmode=disable"
	db, err := sql.Open("pgx", connectionString)
	defer db.Close()

	assert.Nil(t, err)

	_, err = db.Exec("CREATE DATABASE " + dbName)
	assert.Nil(t, err)

	testDb, err := sql.Open("pgx", connectionString+" dbname="+dbName)
	assert.Nil(t, err)

	defer func() {
		err = testDb.Close()
		assert.Nil(t, err)
		// disconnect all db user before cleanup
		_, err = db.Exec(
			`SELECT pg_terminate_backend(pg_stat_activity.pid)
			 FROM pg_stat_activity WHERE pg_stat_activity.datname = '` + dbName + `';`)
		assert.Nil(t, err)
		_, err = db.Exec("DROP DATABASE " + dbName)
		assert.Nil(t, err)
	}()

	err = schema.UpdateDatabase(testDb)
	assert.Nil(t, err)

	action(testDb)
}

func WithDatabasePgx(t *testing.T, action func(db *pgxpool.Pool)) {
	ctx := context.Background()

	// Connect and create a dedicated database for the test
	// For now use database/sql for this
	dbName := "test_" + util.NewULID()
	connectionString := "host=localhost port=5432 user=postgres password=psw sslmode=disable"
	db, err := sql.Open("pgx", connectionString)
	defer db.Close()
	assert.Nil(t, err)
	_, err = db.Exec("CREATE DATABASE " + dbName)
	assert.Nil(t, err)

	// Connect again- this time to the database we just created and using pgx pool.  This will be used for tests
	testDbPool, err := pgxpool.Connect(ctx, connectionString+" dbname="+dbName)
	assert.Nil(t, err)

	defer func() {
		testDbPool.Close()
		// disconnect all db user before cleanup
		_, err = db.Exec(
			`SELECT pg_terminate_backend(pg_stat_activity.pid)
			 FROM pg_stat_activity WHERE pg_stat_activity.datname = '` + dbName + `';`)
		assert.Nil(t, err)
		_, err = db.Exec("DROP DATABASE " + dbName)
		assert.Nil(t, err)
	}()

	// A third connection!  We can get rid of this once we use move udateDatabse over to pgx
	legacyDb, err := sql.Open("pgx", connectionString+" dbname="+dbName)
	defer legacyDb.Close()
	assert.Nil(t, err)
	err = schema.UpdateDatabase(legacyDb)
	assert.Nil(t, err)

	action(testDbPool)
}
