package repository

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/lookout/repository/schema"
	"github.com/G-Research/armada/pkg/api"
)

func Test_RecordEvents(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobRepo := NewSQLJobRepository(db)

		job := &api.Job{
			Id:          util.NewULID(),
			JobSetId:    "job-set",
			Queue:       "queue",
			Namespace:   "nameSpace",
			Labels:      nil,
			Annotations: nil,
			Owner:       "user",
			Priority:    0,
			PodSpec:     &v1.PodSpec{},
			Created:     time.Now(),
		}

		err := jobRepo.RecordJob(job)
		assert.NoError(t, err)

	})
}

func withDatabase(t *testing.T, action func(*sql.DB)) {
	dbName := "test_" + util.NewULID()
	connectionString := "host=localhost port=5432 user=postgres password=psw sslmode=disable"
	db, err := sql.Open("postgres", connectionString)
	defer db.Close()

	assert.Nil(t, err)

	_, err = db.Exec("CREATE DATABASE " + dbName)
	assert.Nil(t, err)

	testDb, err := sql.Open("postgres", connectionString+" dbname="+dbName)
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
