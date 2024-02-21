package queryapi

import (
	"context"

	"github.com/armadaproject/armada/pkg/api"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/armadaproject/armada/internal/armada/queryapi/database"
	"github.com/armadaproject/armada/internal/common/database/lookout"
)

// JobStateMap is a mapping between database state and api states
var JobStateMap = map[int16]api.JobState{
	lookout.JobLeasedOrdinal:    api.JobState_LEASED,
	lookout.JobQueuedOrdinal:    api.JobState_QUEUED,
	lookout.JobPendingOrdinal:   api.JobState_PENDING,
	lookout.JobRunningOrdinal:   api.JobState_RUNNING,
	lookout.JobSucceededOrdinal: api.JobState_SUCCEEDED,
	lookout.JobFailedOrdinal:    api.JobState_FAILED,
	lookout.JobCancelledOrdinal: api.JobState_CANCELLED,
	lookout.JobPreemptedOrdinal: api.JobState_PREEMPTED,
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
		apiStatus = api.JobState_UNKNOWN
	}
	return &api.JobStatusResponse{
		JobId:    req.JobId,
		JobState: apiStatus,
	}, nil
}

func (q *QueryApi) GetJob(ctx context.Context, request *api.JobRequest) (*api.JobResponse, error) {
	// TODO implement me
	panic("implement me")
}
