package main

import (
	"database/sql"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/lookout/repository/schema"
)

const (
	JOBS_EXPECTED = 100
)

func TestDBGen(t *testing.T) {
	db := executablaAndDBsetup(t)

	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM job;").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, JOBS_EXPECTED, count)

	err = db.QueryRow("SELECT COUNT(*) FROM job_run;").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, JOBS_EXPECTED, count)

	truncateDB(t, db)
	db.Close()
	err = exec.Command("rm", "db-gen").Run()
	assert.NoError(t, err)
}

func executablaAndDBsetup(t *testing.T) *sql.DB {
	connectionString := "host=localhost port=5432 user=postgres password=psw sslmode=disable"
	db, err := sql.Open("pgx", connectionString)
	assert.NoError(t, err)

	err = migrateDB(t, db)
	assert.NoError(t, err)

	build := exec.Command("go", "build", "-o", "db-gen", "main.go")
	err = build.Run()
	assert.NoError(t, err)

	db_gen := exec.Command("./db-gen", "-jobs", "100")
	err = db_gen.Run()
	assert.NoError(t, err)

	return db
}

// ensure the db is clear
func truncateDB(t *testing.T, db *sql.DB) {
	_, err := db.Exec("TRUNCATE job;")
	assert.NoError(t, err)
	_, err = db.Exec("TRUNCATE job_run;")
	assert.NoError(t, err)
}

func migrateDB(t *testing.T, db *sql.DB) error {
	return schema.UpdateDatabase(db)
}
