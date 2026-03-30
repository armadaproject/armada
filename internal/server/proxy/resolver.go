package proxy

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/armadaproject/armada/internal/common/database/lookout"
)

// ErrJobNotFound is returned when the requested job_id does not exist.
var ErrJobNotFound = errors.New("job not found")

// ErrJobNotRunning is returned when the job exists but has no active running run.
var ErrJobNotRunning = errors.New("job is not currently running")

// ResolvedJob holds the information needed to route an exec request to the
// correct executor and pod.
type ResolvedJob struct {
	ExecutorID string
	Namespace  string
	RunID      string
	Queue      string
}

// JobResolver looks up running jobs in the Lookout database.
type JobResolver struct {
	db *pgxpool.Pool
}

// NewJobResolver creates a JobResolver backed by the given connection pool.
func NewJobResolver(db *pgxpool.Pool) *JobResolver {
	return &JobResolver{db: db}
}

// ResolveRunningJob returns the executor, namespace, and run ID for the most
// recently started running run of jobID.
//
// Uses lookout.JobRunRunningOrdinal (= 2) to identify running runs.
func (r *JobResolver) ResolveRunningJob(ctx context.Context, jobID string) (*ResolvedJob, error) {
	const query = `
		SELECT jr.run_id, jr.cluster AS executor_id, j.namespace, j.queue
		FROM job_run jr
		JOIN job j ON jr.job_id = j.job_id
		WHERE jr.job_id = $1
		  AND jr.job_run_state = $2
		ORDER BY jr.started DESC
		LIMIT 1`

	row := r.db.QueryRow(ctx, query, jobID, lookout.JobRunRunningOrdinal)

	var res ResolvedJob
	err := row.Scan(&res.RunID, &res.ExecutorID, &res.Namespace, &res.Queue)
	if err != nil {
		// Check if the job exists at all (to give a better error message).
		var count int
		countErr := r.db.QueryRow(ctx,
			"SELECT COUNT(*) FROM job WHERE job_id = $1", jobID).Scan(&count)
		if countErr == nil && count == 0 {
			return nil, ErrJobNotFound
		}
		return nil, ErrJobNotRunning
	}
	return &res, nil
}
