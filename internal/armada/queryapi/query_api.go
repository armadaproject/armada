package queryapi

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/armadaproject/armada/internal/armada/queryapi/database"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/pkg/queryapi"
)

// JobStateMap is a mapping between database state and api states
var JobStateMap = map[int16]queryapi.JobStatus{
	lookout.JobLeasedOrdinal:    queryapi.JobStatus_LEASED,
	lookout.JobQueuedOrdinal:    queryapi.JobStatus_QUEUED,
	lookout.JobPendingOrdinal:   queryapi.JobStatus_PENDING,
	lookout.JobRunningOrdinal:   queryapi.JobStatus_RUNNING,
	lookout.JobSucceededOrdinal: queryapi.JobStatus_SUCCEEDED,
	lookout.JobFailedOrdinal:    queryapi.JobStatus_FAILED,
	lookout.JobCancelledOrdinal: queryapi.JobStatus_CANCELLED,
	lookout.JobPreemptedOrdinal: queryapi.JobStatus_PREEMPTED,
}

type QueryApi struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *QueryApi {
	return &QueryApi{db: db}
}

func (q *QueryApi) GetJobStatus(ctx context.Context, req *queryapi.JobStatusRequest) (*queryapi.JobStatusResponse, error) {
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
		apiStatus = queryapi.JobStatus_UNKNOWN
	}
	return &queryapi.JobStatusResponse{
		JobId:     req.JobId,
		JobStatus: apiStatus,
	}, nil
}
