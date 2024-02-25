package queryapi

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/armada/queryapi/database"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	dbcommon "github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/pkg/api"
)

const (
	defaultMaxQueryItems = 100
)

var (
	baseTime, _      = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	testDecompressor = func() compress.Decompressor { return &compress.NoOpDecompressor{} }
)

func TestGetJobDetails(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	job1 := newJob("job1", lookout.JobQueuedOrdinal)
	job2 := newJob("job2", lookout.JobRunningOrdinal)
	job2.LatestRunID = pointer.String("run1")

	testJobs := []database.Job{job1, job2}

	testJobRuns := []database.JobRun{
		newJobRun("job2", "run1", lookout.JobRunRunningOrdinal, baseTime),
		newJobRun("job2", "run2", lookout.JobRunLeaseReturnedOrdinal, baseTime.Add(-1*time.Minute)),
	}

	// setup job db
	tests := map[string]struct {
		request          *api.JobDetailsRequest
		expectedResponse *api.JobDetailsResponse
	}{
		"single job": {
			request: &api.JobDetailsRequest{
				JobIds: []string{"job1"},
			},
			expectedResponse: &api.JobDetailsResponse{
				JobDetails: map[string]*api.JobDetails{
					"job1": newJobDetails("job1", api.JobState_QUEUED, ""),
				},
			},
		},
		"multiple jobs": {
			request: &api.JobDetailsRequest{
				JobIds: []string{"job1", "job2"},
			},
			expectedResponse: &api.JobDetailsResponse{
				JobDetails: map[string]*api.JobDetails{
					"job1": newJobDetails("job1", api.JobState_QUEUED, ""),
					"job2": newJobDetails("job2", api.JobState_RUNNING, "run1"),
				},
			},
		},
		"no jobs": {
			request: &api.JobDetailsRequest{
				JobIds: []string{},
			},
			expectedResponse: &api.JobDetailsResponse{
				JobDetails: map[string]*api.JobDetails{},
			},
		},
		"non existent job": {
			request: &api.JobDetailsRequest{
				JobIds: []string{"this job doesn't exist!"},
			},
			expectedResponse: &api.JobDetailsResponse{
				JobDetails: map[string]*api.JobDetails{},
			},
		},
		"ask for run but no run available": {
			request: &api.JobDetailsRequest{
				JobIds:       []string{"job1"},
				ExpandJobRun: true,
			},
			expectedResponse: &api.JobDetailsResponse{
				JobDetails: map[string]*api.JobDetails{
					"job1": newJobDetails("job1", api.JobState_QUEUED, ""),
				},
			},
		},
		"ask for runs": {
			request: &api.JobDetailsRequest{
				JobIds:       []string{"job2"},
				ExpandJobRun: true,
			},
			expectedResponse: &api.JobDetailsResponse{
				JobDetails: map[string]*api.JobDetails{
					"job2": newJobDetails(
						"job2",
						api.JobState_RUNNING,
						"run1",
						newJobRunDetails("job2", "run1", api.JobRunState_RUN_STATE_RUNNING, baseTime),
						newJobRunDetails("job2", "run2", api.JobRunState_RUNS_STATE_LEASE_RETURNED, baseTime.Add(-1*time.Minute))),
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
				err := dbcommon.UpsertWithTransaction(ctx, db, "job", testJobs)
				require.NoError(t, err)
				err = dbcommon.UpsertWithTransaction(ctx, db, "job_run", testJobRuns)
				require.NoError(t, err)
				queryApi := New(db, defaultMaxQueryItems, testDecompressor)
				resp, err := queryApi.GetJobDetails(ctx, tc.request)
				require.NoError(t, err)
				assert.Equal(t, tc.expectedResponse, resp)
				return nil
			})
			assert.NoError(t, err)
		})
	}
}

func TestGetJobRunDetails(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	testJobs := []database.Job{
		newJob("job1", lookout.JobRunningOrdinal),
	}

	testJobRuns := []database.JobRun{
		newJobRun("job1", "run1", lookout.JobRunRunningOrdinal, baseTime),
		newJobRun("job1", "run2", lookout.JobRunLeaseReturnedOrdinal, baseTime.Add(-1*time.Minute)),
	}

	// setup job db
	tests := map[string]struct {
		request          *api.JobRunDetailsRequest
		expectedResponse *api.JobRunDetailsResponse
	}{
		"single run": {
			request: &api.JobRunDetailsRequest{
				RunIds: []string{"run1"},
			},
			expectedResponse: &api.JobRunDetailsResponse{
				JobRunDetails: map[string]*api.JobRunDetails{
					"run1": newJobRunDetails("job1", "run1", api.JobRunState_RUN_STATE_RUNNING, baseTime),
				},
			},
		},
		"multiple runs": {
			request: &api.JobRunDetailsRequest{
				RunIds: []string{"run1", "run2"},
			},
			expectedResponse: &api.JobRunDetailsResponse{
				JobRunDetails: map[string]*api.JobRunDetails{
					"run1": newJobRunDetails("job1", "run1", api.JobRunState_RUN_STATE_RUNNING, baseTime),
					"run2": newJobRunDetails("job1", "run2", api.JobRunState_RUNS_STATE_LEASE_RETURNED, baseTime.Add(-1*time.Minute)),
				},
			},
		},
		"no runs": {
			request: &api.JobRunDetailsRequest{
				RunIds: []string{"not a valid run"},
			},
			expectedResponse: &api.JobRunDetailsResponse{
				JobRunDetails: map[string]*api.JobRunDetails{},
			},
		},
		"empty runs": {
			request: &api.JobRunDetailsRequest{
				RunIds: []string{},
			},
			expectedResponse: &api.JobRunDetailsResponse{
				JobRunDetails: map[string]*api.JobRunDetails{},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
				err := dbcommon.UpsertWithTransaction(ctx, db, "job", testJobs)
				require.NoError(t, err)
				err = dbcommon.UpsertWithTransaction(ctx, db, "job_run", testJobRuns)
				require.NoError(t, err)
				queryApi := New(db, defaultMaxQueryItems, testDecompressor)
				resp, err := queryApi.GetJobRunDetails(ctx, tc.request)
				require.NoError(t, err)
				assert.Equal(t, tc.expectedResponse, resp)
				return nil
			})
			assert.NoError(t, err)
		})
	}
}

func TestGetJobStatus(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	testdata := []database.Job{
		newJob("leasedJob", lookout.JobLeasedOrdinal),
		newJob("runningJob", lookout.JobRunningOrdinal),
		newJob("succeededJob", lookout.JobSucceededOrdinal),
	}
	// setup job db
	tests := map[string]struct {
		jobIds           []string
		expectedResponse *api.JobStatusResponse
	}{
		"leased job": {
			jobIds: []string{"leasedJob"},
			expectedResponse: &api.JobStatusResponse{
				JobStates: map[string]api.JobState{
					"leasedJob": api.JobState_LEASED,
				},
			},
		},
		"running job": {
			jobIds: []string{"runningJob"},
			expectedResponse: &api.JobStatusResponse{
				JobStates: map[string]api.JobState{
					"runningJob": api.JobState_RUNNING,
				},
			},
		},
		"succeeded job": {
			jobIds: []string{"succeededJob"},
			expectedResponse: &api.JobStatusResponse{
				JobStates: map[string]api.JobState{
					"succeededJob": api.JobState_SUCCEEDED,
				},
			},
		},
		"multiple jobs": {
			jobIds: []string{"succeededJob", "runningJob"},
			expectedResponse: &api.JobStatusResponse{
				JobStates: map[string]api.JobState{
					"succeededJob": api.JobState_SUCCEEDED,
					"runningJob":   api.JobState_RUNNING,
				},
			},
		},
		"missing job": {
			jobIds: []string{"missingJob"},
			expectedResponse: &api.JobStatusResponse{
				JobStates: map[string]api.JobState{
					"missingJob": api.JobState_UNKNOWN,
				},
			},
		},
		"no jobs": {
			jobIds: []string{},
			expectedResponse: &api.JobStatusResponse{
				JobStates: map[string]api.JobState{},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
				err := dbcommon.UpsertWithTransaction(ctx, db, "job", testdata)
				require.NoError(t, err)
				queryApi := New(db, defaultMaxQueryItems, testDecompressor)
				resp, err := queryApi.GetJobStatus(ctx, &api.JobStatusRequest{JobIds: tc.jobIds})
				require.NoError(t, err)
				assert.Equal(t, tc.expectedResponse, resp)
				return nil
			})
			assert.NoError(t, err)
		})
	}
}

func newJob(jobId string, state int16) database.Job {
	return database.Job{
		JobID:            jobId,
		Queue:            "testQueue",
		Owner:            "testOwner",
		Jobset:           "testJobset",
		Cpu:              0,
		Memory:           0,
		EphemeralStorage: 0,
		Gpu:              0,
		Priority:         0,
		Submitted: pgtype.Timestamp{
			Time:  baseTime,
			Valid: true,
		},
		Cancelled: pgtype.Timestamp{},
		State:     state,
		LastTransitionTime: pgtype.Timestamp{
			Time:  baseTime,
			Valid: true,
		},
		LastTransitionTimeSeconds: 0,
		JobSpec:                   []byte{},
		Duplicate:                 false,
		PriorityClass:             nil,
		LatestRunID:               nil,
		CancelReason:              nil,
		Namespace:                 pointer.String("testNamespace"),
		Annotations:               nil,
	}
}

func newJobRun(jobId, runId string, state int16, leased time.Time) database.JobRun {
	return database.JobRun{
		RunID:   runId,
		JobID:   jobId,
		Cluster: "testCluster",
		Node:    pointer.String("testNode"),
		Pending: pgtype.Timestamp{
			Time:  baseTime,
			Valid: true,
		},
		Started: pgtype.Timestamp{
			Time:  baseTime,
			Valid: true,
		},
		Finished:    pgtype.Timestamp{},
		JobRunState: state,
		Error:       nil,
		ExitCode:    nil,
		Leased: pgtype.Timestamp{
			Time:  leased,
			Valid: true,
		},
	}
}

func newJobDetails(jobId string, state api.JobState, latestRunId string, runs ...*api.JobRunDetails) *api.JobDetails {
	return &api.JobDetails{
		JobId:            jobId,
		Queue:            "testQueue",
		Jobset:           "testJobset",
		Namespace:        "testNamespace",
		State:            state,
		SubmittedTs:      &baseTime,
		CancelTs:         nil,
		CancelReason:     "",
		LastTransitionTs: &baseTime,
		LatestRunId:      latestRunId,
		JobSpec:          nil,
		JobRuns:          runs,
	}
}

func newJobRunDetails(jobId string, runId string, state api.JobRunState, leased time.Time) *api.JobRunDetails {
	return &api.JobRunDetails{
		RunId:      runId,
		JobId:      jobId,
		State:      state,
		Cluster:    "testCluster",
		Node:       "testNode",
		LeasedTs:   &leased,
		PendingTs:  &baseTime,
		StartedTs:  &baseTime,
		FinishedTs: nil,
	}
}
