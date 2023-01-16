package repository

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/lookout/testutil"
)

var startDate, _ = time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")

func Test_HappyPath(t *testing.T) {
	t.Run("delete half", func(t *testing.T) {
		withPopulatedDatabase(t, func(db *sql.DB) {
			err := DeleteOldJobs(db, 10, startDate.AddDate(0, 0, 5))
			assert.NoError(t, err)
			expectedIds := []string{"job-5", "job-6", "job-7", "job-8", "job-9"}
			assert.Equal(t, expectedIds, queryForIds(t, db, "job"))
			assert.Equal(t, expectedIds, queryForIds(t, db, "job_run"))
			assert.Equal(t, expectedIds, queryForIds(t, db, "user_annotation_lookup"))
		})
	})

	t.Run("delete nothing but null", func(t *testing.T) {
		withPopulatedDatabase(t, func(db *sql.DB) {
			err := DeleteOldJobs(db, 10, startDate.AddDate(0, 0, -1))
			assert.NoError(t, err)
			expectedIds := []string{"job-0", "job-1", "job-2", "job-3", "job-4", "job-5", "job-6", "job-7", "job-8", "job-9"}
			assert.Equal(t, expectedIds, queryForIds(t, db, "job"))
			assert.Equal(t, expectedIds, queryForIds(t, db, "job_run"))
			assert.Equal(t, expectedIds, queryForIds(t, db, "user_annotation_lookup"))
		})
	})

	t.Run("delete everything", func(t *testing.T) {
		withPopulatedDatabase(t, func(db *sql.DB) {
			err := DeleteOldJobs(db, 10, startDate.AddDate(0, 0, 11))
			assert.NoError(t, err)
			expectedIds := []string(nil)
			assert.Equal(t, expectedIds, queryForIds(t, db, "job"))
			assert.Equal(t, expectedIds, queryForIds(t, db, "job_run"))
			assert.Equal(t, expectedIds, queryForIds(t, db, "user_annotation_lookup"))
		})
	})
}

func Test_BatchSize(t *testing.T) {
	t.Run("different batch sizes", func(t *testing.T) {
		batchSizes := []int{1, 2, 5, 10, 100}
		for _, size := range batchSizes {
			withPopulatedDatabase(t, func(db *sql.DB) {
				err := DeleteOldJobs(db, size, startDate.AddDate(0, 0, 5))
				assert.NoError(t, err)
				expectedIds := []string{"job-5", "job-6", "job-7", "job-8", "job-9"}
				assert.Equal(t, expectedIds, queryForIds(t, db, "job"))
				assert.Equal(t, expectedIds, queryForIds(t, db, "job_run"))
				assert.Equal(t, expectedIds, queryForIds(t, db, "user_annotation_lookup"))
			})
		}
	})
}

func queryForIds(t *testing.T, db *sql.DB, table string) []string {
	var ids []string
	rows, err := db.Query(fmt.Sprintf("SELECT job_id from %v", table))
	if ok := assert.NoError(t, err); !ok {
		t.FailNow()
	}
	defer rows.Close()
	var id string
	for rows.Next() {
		err := rows.Scan(&id)
		assert.NoError(t, err)
		ids = append(ids, id)
	}
	return ids
}

func withPopulatedDatabase(t *testing.T, action func(db *sql.DB)) {
	err := testutil.WithDatabase(func(db *sql.DB) error {
		for i := 0; i < 10; i++ {
			jobId := fmt.Sprintf("job-%v", i)
			runId := fmt.Sprintf("job-%v-run", i)
			jobSetId := fmt.Sprintf("jobset-%v", i)
			updateTime := startDate.AddDate(0, 0, i)
			queue := "testqueue"
			container := "container"

			_, err := db.Exec(
				"INSERT INTO job (job_id, queue, jobset, submitted) VALUES ($1, $2, $3, $4)",
				jobId, jobSetId, queue, updateTime)
			assert.NoError(t, err)

			_, err2 := db.Exec(
				"INSERT INTO job_run (run_id, job_id) VALUES ($1, $2)",
				runId, jobId)
			assert.NoError(t, err2)

			_, err3 := db.Exec(
				"INSERT INTO job_run_container (run_id, container_name, exit_code) VALUES ($1, $2, $3)",
				runId, container, 0)
			assert.NoError(t, err3)

			_, err4 := db.Exec(
				"INSERT INTO user_annotation_lookup (job_id, key, value) VALUES ($1, $2, $3)",
				jobId, "foo", "bar")
			assert.NoError(t, err4)
		}

		// Extra job will a null submitted time.  This should always be deleted
		_, err := db.Exec(
			"INSERT INTO job (job_id, queue, jobset, submitted) VALUES ($1, $2, $3, $4)",
			"null-submitted", queue, "test-jobset", nil)
		assert.NoError(t, err)

		action(db)
		return nil
	})
	assert.NoError(t, err)
}
