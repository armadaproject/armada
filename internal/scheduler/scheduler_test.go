package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/scheduler/database"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// Data to be used in tests
const maxLeaseReturns = 1

var queuedJob = SchedulerJob{
	JobId:  util.NewULID(),
	Queue:  "testQueue",
	Jobset: "testJobset",
	Queued: true,
}

var leasedJob = SchedulerJob{
	JobId:  util.NewULID(),
	Queue:  "testQueue",
	Jobset: "testJobset",
	Queued: false,
	Runs: []*JobRun{
		{
			RunID:    uuid.New(),
			Executor: "testExecutor",
		},
	},
}

// Test a single scheduler cycle
func TestCycle(t *testing.T) {
	tests := map[string]struct {
		initialJobs          []*SchedulerJob
		jobUpdates           []database.Job
		runUpdates           []database.Run
		staleExecutor        bool
		fetchError           bool
		scheduleError        bool
		publishError         bool
		expectedJobRunLeased []string
		expectedJobRunErrors []string
		expectedJobErrors    []string
		expectedJobCancelled []string
		expectedJobSucceeded []string
		expectedLeased       []string
		expectedQueued       []string
	}{
		"Lease a single job already in the db": {
			initialJobs:          []*SchedulerJob{&queuedJob},
			expectedJobRunLeased: []string{queuedJob.JobId},
			expectedLeased:       []string{queuedJob.JobId},
		},
		"Lease a single job from an update": {
			jobUpdates: []database.Job{
				{
					JobID:  queuedJob.JobId,
					JobSet: "testJobSet",
					Queue:  "testQueue",
					Serial: 1,
				},
			},
			expectedJobRunLeased: []string{queuedJob.JobId},
			expectedLeased:       []string{queuedJob.JobId},
		},
		"Nothing leased": {
			initialJobs:    []*SchedulerJob{&queuedJob},
			expectedQueued: []string{queuedJob.JobId},
		},
		"No updates to an already leased job": {
			initialJobs:    []*SchedulerJob{&leasedJob},
			expectedLeased: []string{leasedJob.JobId},
		},
		"Lease returned and re-queued": {
			initialJobs: []*SchedulerJob{&leasedJob},
			runUpdates: []database.Run{
				{
					RunID:    leasedJob.Runs[0].RunID,
					JobID:    leasedJob.JobId,
					JobSet:   "testJobSet",
					Executor: "testExecutor",
					Failed:   true,
					Returned: true,
					Serial:   1,
				},
			},
			expectedQueued: []string{leasedJob.JobId},
		},
		"Lease returned too many times": {
			initialJobs: []*SchedulerJob{&leasedJob},
			// 2 failures here so the second one should trigger a run failure
			runUpdates: []database.Run{
				{
					RunID:    leasedJob.Runs[0].RunID,
					JobID:    leasedJob.JobId,
					JobSet:   "testJobSet",
					Executor: "testExecutor",
					Failed:   true,
					Returned: true,
					Serial:   1,
				},
				{
					RunID:    uuid.New(),
					JobID:    leasedJob.JobId,
					JobSet:   "testJobSet",
					Executor: "testExecutor",
					Failed:   true,
					Returned: true,
					Serial:   2,
				},
			},
			expectedJobErrors: []string{leasedJob.JobId},
		},
		"Job cancelled": {
			initialJobs: []*SchedulerJob{&queuedJob},
			jobUpdates: []database.Job{
				{
					JobID:           queuedJob.JobId,
					JobSet:          "testJobSet",
					Queue:           "testQueue",
					CancelRequested: true,
					Serial:          1,
				},
			},
			expectedJobCancelled: []string{queuedJob.JobId},
		},
		"Lease expired": {
			initialJobs:          []*SchedulerJob{&leasedJob},
			staleExecutor:        true,
			expectedJobRunErrors: []string{leasedJob.JobId},
			expectedJobErrors:    []string{leasedJob.JobId},
		},
		"Job failed": {
			initialJobs: []*SchedulerJob{&leasedJob},
			runUpdates: []database.Run{
				{
					RunID:    leasedJob.Runs[0].RunID,
					JobID:    leasedJob.JobId,
					JobSet:   "testJobSet",
					Executor: "testExecutor",
					Failed:   true,
					Serial:   1,
				},
			},
			expectedJobErrors: []string{leasedJob.JobId},
		},
		"Job succeeded": {
			initialJobs: []*SchedulerJob{&leasedJob},
			runUpdates: []database.Run{
				{
					RunID:     leasedJob.Runs[0].RunID,
					JobID:     leasedJob.JobId,
					JobSet:    "testJobSet",
					Executor:  "testExecutor",
					Succeeded: true,
					Serial:    1,
				},
			},
			expectedJobSucceeded: []string{leasedJob.JobId},
		},
		"Fetch fails": {
			initialJobs:    []*SchedulerJob{&leasedJob},
			fetchError:     true,
			expectedLeased: []string{leasedJob.JobId},
		},
		"Schedule fails": {
			initialJobs:    []*SchedulerJob{&leasedJob},
			scheduleError:  true,
			staleExecutor:  true,
			expectedLeased: []string{leasedJob.JobId}, // job should still be leased as error was thrown and transaction rolled back
		},
		"Publish fails": {
			initialJobs:    []*SchedulerJob{&leasedJob},
			publishError:   true,
			staleExecutor:  true,
			expectedLeased: []string{leasedJob.JobId}, // job should still be leased as error was thrown and transaction rolled back
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			clusterTimeout := 1 * time.Hour

			// Test objects
			jobRepo := testJobRepository{
				updatedJobs: tc.jobUpdates,
				updatedRuns: tc.runUpdates,
				shouldError: tc.fetchError,
			}
			testClock := clock.NewFakeClock(time.Now())
			schedulingAlgo := &testSchedulingAlgo{jobsToSchedule: tc.expectedJobRunLeased, shouldError: tc.scheduleError}
			publisher := &testPublisher{shouldError: tc.publishError}
			heartbeatTime := testClock.Now()
			if tc.staleExecutor {
				heartbeatTime = heartbeatTime.Add(-2 * clusterTimeout)
			}
			clusterRepo := &testExecutorRepository{
				updateTimes: map[string]time.Time{"testExecutor": heartbeatTime},
			}
			sched, err := NewScheduler(
				&jobRepo,
				clusterRepo,
				schedulingAlgo,
				NewStandaloneLeaderController(),
				publisher,
				1*time.Second,
				clusterTimeout,
				maxLeaseReturns)
			require.NoError(t, err)

			sched.clock = testClock

			// insert initial jobs
			txn := sched.jobDb.WriteTxn()
			err = sched.jobDb.Upsert(txn, tc.initialJobs)
			require.NoError(t, err)
			txn.Commit()

			// run a scheduler cycle
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = sched.cycle(ctx, false, sched.leaderController.GetToken())
			if tc.fetchError || tc.publishError || tc.scheduleError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// assert that every event we've generated was expected
			outstandingLeaseMessages := stringSet(tc.expectedJobRunLeased)
			outstandingJobErrorMessages := stringSet(tc.expectedJobErrors)
			outstandingJobRunErrorMessages := stringSet(tc.expectedJobRunErrors)
			outstandingCancelledMessages := stringSet(tc.expectedJobCancelled)
			outstandingJobSucceededMessages := stringSet(tc.expectedJobSucceeded)
			for _, event := range publisher.events {
				for _, e := range event.Events {
					if e.GetJobRunLeased() != nil {
						leased := e.GetJobRunLeased()
						jobId, err := armadaevents.UlidStringFromProtoUuid(leased.JobId)
						require.NoError(t, err)
						_, ok := outstandingLeaseMessages[jobId]
						assert.True(t, ok)
						delete(outstandingLeaseMessages, jobId)
					} else if e.GetJobErrors() != nil {
						jobErrors := e.GetJobErrors()
						jobId, err := armadaevents.UlidStringFromProtoUuid(jobErrors.JobId)
						require.NoError(t, err)
						_, ok := outstandingJobErrorMessages[jobId]
						assert.True(t, ok)
						delete(outstandingJobErrorMessages, jobId)
					} else if e.GetJobRunErrors() != nil {
						jobErrors := e.GetJobRunErrors()
						jobId, err := armadaevents.UlidStringFromProtoUuid(jobErrors.JobId)
						require.NoError(t, err)
						_, ok := outstandingJobRunErrorMessages[jobId]
						assert.True(t, ok)
						delete(outstandingJobRunErrorMessages, jobId)
					} else if e.GetJobSucceeded() != nil {
						jobErrors := e.GetJobSucceeded()
						jobId, err := armadaevents.UlidStringFromProtoUuid(jobErrors.JobId)
						require.NoError(t, err)
						_, ok := outstandingJobSucceededMessages[jobId]
						assert.True(t, ok)
						delete(outstandingJobSucceededMessages, jobId)
					} else if e.GetCancelledJob() != nil {
						jobErrors := e.GetCancelledJob()
						jobId, err := armadaevents.UlidStringFromProtoUuid(jobErrors.JobId)
						require.NoError(t, err)
						_, ok := outstandingCancelledMessages[jobId]
						assert.True(t, ok)
						delete(outstandingCancelledMessages, jobId)
					} else {
						assert.Fail(t, fmt.Sprintf("unknown event sent to publisher %+v", e))
					}
				}
			}
			// Assert that we didn't miss out any events
			assert.Equal(t, 0, len(outstandingLeaseMessages))
			assert.Equal(t, 0, len(outstandingJobErrorMessages))
			assert.Equal(t, 0, len(outstandingJobRunErrorMessages))
			assert.Equal(t, 0, len(outstandingCancelledMessages))
			assert.Equal(t, 0, len(outstandingJobSucceededMessages))

			// assert that the serials are where we expect them to be
			if len(tc.jobUpdates) > 0 {
				assert.Equal(t, tc.jobUpdates[len(tc.jobUpdates)-1].Serial, sched.jobsSerial)
			} else {
				assert.Equal(t, int64(-1), sched.jobsSerial)
			}
			if len(tc.runUpdates) > 0 {
				assert.Equal(t, tc.runUpdates[len(tc.runUpdates)-1].Serial, sched.runsSerial)
			} else {
				assert.Equal(t, int64(-1), sched.runsSerial)
			}

			// assert that the job db is in the state we expect
			jobs, err := sched.jobDb.GetAll(sched.jobDb.ReadTxn())
			require.NoError(t, err)
			remainingLeased := stringSet(tc.expectedLeased)
			remainingQueued := stringSet(tc.expectedQueued)
			for _, job := range jobs {
				if job.Queued {
					_, ok := remainingQueued[job.JobId]
					assert.True(t, ok)
					delete(remainingQueued, job.JobId)
				} else {
					_, ok := remainingLeased[job.JobId]
					assert.True(t, ok)
					delete(remainingLeased, job.JobId)
				}
			}
			assert.Equal(t, 0, len(remainingLeased))
			assert.Equal(t, 0, len(remainingQueued))
			cancel()
		})
	}
}

type testJobRepository struct {
	updatedJobs []database.Job
	updatedRuns []database.Run
	errors      map[uuid.UUID]*armadaevents.JobRunErrors
	shouldError bool
}

func (t *testJobRepository) FetchJobUpdates(ctx context.Context, jobSerial int64, jobRunSerial int64) ([]database.Job, []database.Run, error) {
	if t.shouldError {
		return nil, nil, errors.New("error fetchiung job updates")
	}
	return t.updatedJobs, t.updatedRuns, nil
}

func (t *testJobRepository) FetchJobRunErrors(ctx context.Context, runIds []uuid.UUID) (map[uuid.UUID]*armadaevents.JobRunErrors, error) {
	if t.shouldError {
		return nil, errors.New("error fetching job run errors")
	}
	return t.errors, nil
}

func (t *testJobRepository) CountReceivedPartitions(ctx context.Context, groupId uuid.UUID) (uint32, error) {
	panic("CountReceivedPartitions not implemented yet")
}

type testExecutorRepository struct {
	updateTimes map[string]time.Time
	shouldError bool
}

func (t testExecutorRepository) GetExecutors() ([]*database.Executor, error) {
	panic("GetExecutors not implemented yet")
}

func (t testExecutorRepository) GetLastUpdateTimes() (map[string]time.Time, error) {
	if t.shouldError {
		return nil, errors.New("error getting last update time")
	}
	return t.updateTimes, nil
}

type testSchedulingAlgo struct {
	jobsToSchedule []string
	shouldError    bool
}

func (t *testSchedulingAlgo) Schedule(txn *memdb.Txn, jobDb *JobDb) ([]*SchedulerJob, error) {
	if t.shouldError {
		return nil, errors.New("error scheduling jobs")
	}
	jobs := make([]*SchedulerJob, 0, len(t.jobsToSchedule))
	for _, id := range t.jobsToSchedule {
		job, _ := jobDb.GetById(txn, id)
		if job != nil {
			if !job.Queued {
				return nil, errors.New(fmt.Sprintf("Was asked to lease %s but was already leased", job.JobId))
			}
			job = job.DeepCopy()
			job.Queued = false
			job.Runs = append(job.Runs, &JobRun{
				RunID:    uuid.New(),
				Executor: "test-executor",
			})
			jobs = append(jobs, job)
		} else {
			return nil, errors.New(fmt.Sprintf("Was asked to lease %s but job does not exist", job.JobId))
		}
	}
	if len(jobs) > 0 {
		jobDb.Upsert(txn, jobs)
	}
	return jobs, nil
}

type testPublisher struct {
	events      []*armadaevents.EventSequence
	shouldError bool
}

func (t *testPublisher) PublishMessages(ctx context.Context, events []*armadaevents.EventSequence, _ LeaderToken) error {
	if t.shouldError {
		return errors.New("Error when publishing")
	}
	t.events = events
	return nil
}

func (t *testPublisher) PublishMarkers(ctx context.Context, groupId uuid.UUID) (uint32, error) {
	panic("PublishMarkers not implemented yet")
}

func stringSet(src []string) map[string]bool {
	set := make(map[string]bool, len(src))
	for _, s := range src {
		set[s] = true
	}
	return set
}
