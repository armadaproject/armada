package queryapi

import (
	"context"

	"github.com/armadaproject/armada/pkg/api"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/armadaproject/armada/internal/armada/queryapi/database"
	"github.com/armadaproject/armada/internal/common/database/lookout"
)

// JobStateMap is a mapping between database state and api states
var JobStateMap = map[int16]api.JobStatus{
	lookout.JobLeasedOrdinal:    api.JobStatus_LEASED,
	lookout.JobQueuedOrdinal:    api.JobStatus_QUEUED,
	lookout.JobPendingOrdinal:   api.JobStatus_PENDING,
	lookout.JobRunningOrdinal:   api.JobStatus_RUNNING,
	lookout.JobSucceededOrdinal: api.JobStatus_SUCCEEDED,
	lookout.JobFailedOrdinal:    api.JobStatus_FAILED,
	lookout.JobCancelledOrdinal: api.JobStatus_CANCELLED,
	lookout.JobPreemptedOrdinal: api.JobStatus_PREEMPTED,
}

type QueryApi struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *QueryApi {
	return &QueryApi{db: db}
}

func (q *QueryApi) GetJobStatus(ctx context.Context, req *api.JobStatusRequest) (*api.JobStatusResponse, error) {
	queries := database.New(q.db)
	queryResult, err := queries.GetJobState(ctx, req.JobId)
	if err != nil {
		return nil, err
	}
	status := int16(-1)
	if len(queryResult) > 0 {
		status = queryResult[0]
	}
	apiStatus, ok := JobStateMap[status]
	if !ok {
		apiStatus = api.JobStatus_UNKNOWN
	}
	return &api.JobStatusResponse{
		JobId:     req.JobId,
		JobStatus: apiStatus,
	}, nil
}
