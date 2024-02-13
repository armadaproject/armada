package queryapi

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/pkg/api"
)

func TestGetJobStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// setup job db
	tests := map[string]struct {
		jobId            string
		expectedResponse *api.JobStatusResponse
	}{
		"leased job": {
			jobId:            "leasedJob",
			expectedResponse: &api.JobStatusResponse{JobId: "leasedJob", JobStatus: api.JobStatus_LEASED},
		},
		"running job": {
			jobId:            "runningJob",
			expectedResponse: &api.JobStatusResponse{JobId: "runningJob", JobStatus: api.JobStatus_RUNNING},
		},
		"completed job": {
			jobId:            "completedJob",
			expectedResponse: &api.JobStatusResponse{JobId: "completedJob", JobStatus: api.JobStatus_SUCCEEDED},
		},
		"missing job": {
			jobId:            "missingJob",
			expectedResponse: &api.JobStatusResponse{JobId: "missingJob", JobStatus: api.JobStatus_UNKNOWN},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
				queryApi := New(db)
				err := insertTestData(ctx, db)
				require.NoError(t, err)
				resp, err := queryApi.GetJobStatus(context.Background(), &api.JobStatusRequest{JobId: tc.jobId})
				require.NoError(t, err)
				assert.Equal(t, resp, tc.expectedResponse)
				return nil
			})
			assert.NoError(t, err)
		})
	}
}

func insertTestData(ctx context.Context, db *pgxpool.Pool) error {
	err := insertJob(ctx, db, "leasedJob", lookout.JobLeasedOrdinal)
	if err != nil {
		return err
	}
	err = insertJob(ctx, db, "runningJob", lookout.JobRunningOrdinal)
	if err != nil {
		return err
	}
	err = insertJob(ctx, db, "completedJob", lookout.JobSucceededOrdinal)
	if err != nil {
		return err
	}
	return nil
}

func insertJob(ctx context.Context, db *pgxpool.Pool, jobId string, state int16) error {
	// Prepare the SQL query
	sql := `INSERT INTO job(job_id, queue, owner, jobset, cpu, memory, ephemeral_storage, gpu, priority, submitted, state, last_transition_time, last_transition_time_seconds, job_spec, duplicate)
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`

	// Prepare dummy values
	queue := " "
	owner := " "
	jobset := " "
	cpu := int64(0)
	memory := int64(0)
	ephemeralStorage := int64(0)
	gpu := int64(0)
	priority := int64(0)
	submitted := time.Now()
	lastTransitionTime := time.Now()
	lastTransitionTimeSeconds := int64(0)
	jobSpec := []byte{}
	duplicate := false

	// Execute the query with the prepared dummy values
	_, err := db.Exec(
		ctx,
		sql,
		jobId,
		queue,
		owner,
		jobset,
		cpu,
		memory,
		ephemeralStorage,
		gpu,
		priority,
		submitted,
		state,
		lastTransitionTime,
		lastTransitionTimeSeconds,
		jobSpec,
		duplicate)

	// Return any error that might have occurred during the execution
	return err
}
