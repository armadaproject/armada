package queryapi

import (
	"github.com/armadaproject/armada/internal/armada/queryapi/database"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/pointer"
	"testing"
	"time"

	dbcommon "github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/pkg/api"
)

var (
	baseTime         = time.Now().UTC()
	testDecompressor = func() compress.Decompressor { return &compress.NoOpDecompressor{} }
)

func TestGetJobDetails(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	testJobs := []database.Job{
		newJob("job1", lookout.JobLeasedOrdinal),
		newJob("job2", lookout.JobRunningOrdinal),
	}

	testJobDetails := []*api.JobDetails{
		newJobDetails("job1", lookout.JobQueuedOrdinal),
		newJobDetails("job1", lookout.JobRunningOrdinal),
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
				Details: map[string]*api.JobDetails{
					"job1": testJobDetails[0],
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
				err := dbcommon.UpsertWithTransaction(ctx, db, "job", testJobs)
				require.NoError(t, err)
				queryApi := New(db, testDecompressor)
				resp, err := queryApi.GetJobDetails(ctx, tc.request)
				require.NoError(t, err)
				assert.Equal(t, resp, tc.expectedResponse)
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
				queryApi := New(db, testDecompressor)
				resp, err := queryApi.GetJobStatus(ctx, &api.JobStatusRequest{JobIds: tc.jobIds})
				require.NoError(t, err)
				assert.Equal(t, resp, tc.expectedResponse)
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

func newJobDetails(jobId string, state api.JobState) *api.JobDetails {
	return &api.JobDetails{
		JobId:            jobId,
		Queue:            "testQueue",
		Jobset:           "testJobset",
		Namespace:        "testNamespace",
		State:            state,
		SubmittedTs:      &baseTime,
		CancelTs:         nil,
		CancelReason:     "",
		LastTransitionTs: nil,
		LatestRunId:      "",
		JobSpec:          nil,
		JobRuns:          nil,
	}
}

func withJobState(job database.Job, state int16) database.Job {
	job.State = state
	return job
}
