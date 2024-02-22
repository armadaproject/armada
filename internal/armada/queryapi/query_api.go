package queryapi

import (
	"context"
	"github.com/armadaproject/armada/internal/armada/queryapi/database"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/jackc/pgx/v5/pgxpool"
)

// JobStateMap is a mapping between database state and api Job states
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

// JobRunStateMap is a mapping between database state and api Job Run states
var JobRunStateMap = map[int16]api.JobRunState{
	lookout.JobRunLeasedOrdinal:    api.JobRunState_RUN_STATE_LEASED,
	lookout.JobRunPendingOrdinal:   api.JobRunState_RUN_STATE_PENDING,
	lookout.JobRunRunningOrdinal:   api.JobRunState_RUN_STATE_RUNNING,
	lookout.JobRunSucceededOrdinal: api.JobRunState_RUN_STATE_SUCCEEDED,
	lookout.JobRunFailedOrdinal:    api.JobRunState_RUN_STATE_FAILED,
	// Lookout seems to have no concept of cancelling runs!
	// lookout.JobRunC:               api.JobRunState_RUN_STATE_CANCELLED,
	lookout.JobRunPreemptedOrdinal:     api.JobRunState_RUN_STATE_PREEMPTED,
	lookout.JobRunLeaseExpiredOrdinal:  api.JobRunState_RUN_STATE_LEASE_EXPIRED,
	lookout.JobRunLeaseReturnedOrdinal: api.JobRunState_RUNS_STATE_LEASE_RETURNED,
}

type QueryApi struct {
	db                  *pgxpool.Pool
	deCompressorFactory func() compress.Decompressor
}

func New(db *pgxpool.Pool, deCompressorFactory func() compress.Decompressor) *QueryApi {
	return &QueryApi{
		db:                  db,
		deCompressorFactory: deCompressorFactory,
	}
}

func (q *QueryApi) GetJobDetails(ctx context.Context, req *api.JobDetailsRequest) (*api.JobDetailsResponse, error) {
	queries := database.New(q.db)

	// Fetch the Job Rows
	resultRows, err := queries.GetJobDetails(ctx, req.JobIds)
	if err != nil {
		return nil, err
	}
	detailsById := make(map[string]*api.JobDetails, len(resultRows))
	jobsWithRuns := make([]string, 0, len(resultRows))
	decompressor := q.deCompressorFactory()
	for _, row := range resultRows {
		var jobSpec *api.Job = nil
		if req.ExpandJobSpec {
			jobSpec, err = protoutil.DecompressAndUnmarshall[*api.Job](row.JobSpec, &api.Job{}, decompressor)
			if err != nil {
				return nil, err
			}
		}
		apiJobState, ok := JobStateMap[row.State]
		if !ok {
			apiJobState = api.JobState_UNKNOWN
		}
		detailsById[row.JobID] = &api.JobDetails{
			JobId:            row.JobID,
			Queue:            row.Queue,
			Jobset:           row.Jobset,
			Namespace:        NilStringToString(row.Namespace),
			State:            apiJobState,
			SubmittedTs:      DbTimeToGoTime(row.Submitted),
			CancelTs:         DbTimeToGoTime(row.Cancelled),
			CancelReason:     NilStringToString(row.CancelReason),
			LastTransitionTs: DbTimeToGoTime(row.LastTransitionTime),
			LatestRunId:      NilStringToString(row.LatestRunID),
			JobSpec:          jobSpec,
		}
		if req.GetExpandJobRun() && row.LatestRunID != nil {
			jobsWithRuns = append(jobsWithRuns, row.JobID)
		}
	}

	// Fetch the Job run details in a separate query.
	// We do this because each job can have many runs and so we don;t want to duplicate the job data for each run
	if len(jobsWithRuns) > 0 {
		runResultRows, err := queries.GetJobRunsByJobIds(ctx, jobsWithRuns)
		if err != nil {
			return nil, err
		}
		runsByJob := make(map[string][]*api.JobRunDetails, len(resultRows))
		for _, row := range runResultRows {
			jobRuns, ok := runsByJob[row.JobID]
			if !ok {
				jobRuns = []*api.JobRunDetails{}
			}
			jobRuns = append(jobRuns, &api.JobRunDetails{
				RunId:      row.RunID,
				JobId:      row.JobID,
				State:      0,
				Cluster:    row.Cluster,
				Node:       NilStringToString(row.Node),
				LeasedTs:   DbTimeToGoTime(row.Leased),
				PendingTs:  DbTimeToGoTime(row.Pending),
				StartedTs:  DbTimeToGoTime(row.Started),
				FinishedTs: DbTimeToGoTime(row.Finished),
			})
			runsByJob[row.RunID] = jobRuns
		}

		for jobId, jobDetails := range detailsById {
			runs, ok := runsByJob[jobId]
			if ok {
				jobDetails.JobRuns = runs
			}
		}
	}

	return &api.JobDetailsResponse{
		Details: detailsById,
	}, nil
}

func (q *QueryApi) GetJobRunDetails(ctx context.Context, req *api.JobRunDetailsRequest) (*api.JobRunDetailsResponse, error) {
	queries := database.New(q.db)
	resultRows, err := queries.GetJobRunsByRunIds(ctx, req.RunIds)
	if err != nil {
		return nil, err
	}
	detailsById := make(map[string]*api.JobRunDetails, len(resultRows))
	for _, row := range resultRows {
		detailsById[row.RunID] = &api.JobRunDetails{
			RunId:      row.RunID,
			JobId:      row.JobID,
			State:      0,
			Cluster:    row.Cluster,
			Node:       NilStringToString(row.Node),
			LeasedTs:   DbTimeToGoTime(row.Leased),
			PendingTs:  DbTimeToGoTime(row.Pending),
			StartedTs:  DbTimeToGoTime(row.Started),
			FinishedTs: DbTimeToGoTime(row.Finished),
		}
	}
	return &api.JobRunDetailsResponse{
		JobRunDetails: detailsById,
	}, nil
}

func (q *QueryApi) GetJobStatus(ctx context.Context, req *api.JobStatusRequest) (*api.JobStatusResponse, error) {
	queries := database.New(q.db)
	queryResult, err := queries.GetJobStates(ctx, req.JobIds)
	if err != nil {
		return nil, err
	}
	dbStatusById := make(map[string]int16, len(queryResult))
	for _, dbRow := range queryResult {
		dbStatusById[dbRow.JobID] = dbRow.State
	}

	apiStatusById := make(map[string]api.JobState, len(queryResult))
	for _, jobId := range req.JobIds {
		dbStatus, ok := dbStatusById[jobId]
		if ok {
			apiStatus, ok := JobStateMap[dbStatus]
			if !ok {
				apiStatus = api.JobState_UNKNOWN // We know about this job but we can't map its state
			}
			apiStatusById[jobId] = apiStatus
		} else {
			apiStatusById[jobId] = api.JobState_UNKNOWN // We don't know about this job
		}
	}

	return &api.JobStatusResponse{
		JobStates: apiStatusById,
	}, nil
}

func decompress(b []byte, decompressor compress.Decompressor) (string, error) {
	if len(b) == 0 {
		return "", nil
	}
	decompressed, err := decompressor.Decompress(b)
	if err != nil {
		return "", err
	}
	return string(decompressed), nil
}
