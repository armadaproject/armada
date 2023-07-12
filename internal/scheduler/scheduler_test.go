package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/clock"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/kubernetesobjects/affinity"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Data to be used in tests
const (
	maxNumberOfAttempts = 2
	nodeIdLabel         = "kubernetes.io/hostname"
)

var (
	schedulingInfo = &schedulerobjects.JobSchedulingInfo{
		AtMostOnce: true,
		ObjectRequirements: []*schedulerobjects.ObjectRequirements{
			{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: &schedulerobjects.PodRequirements{
						Priority: int32(10),
					},
				},
			},
		},
		Version: 1,
	}
	schedulingInfoBytes   = protoutil.MustMarshall(schedulingInfo)
	updatedSchedulingInfo = &schedulerobjects.JobSchedulingInfo{
		AtMostOnce: true,
		ObjectRequirements: []*schedulerobjects.ObjectRequirements{
			{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: &schedulerobjects.PodRequirements{
						Priority: int32(10),
					},
				},
			},
		},
		Version: 2,
	}
	updatedSchedulingInfoBytes = protoutil.MustMarshall(updatedSchedulingInfo)
)

var queuedJob = jobdb.NewJob(
	util.NewULID(),
	"testJobset",
	"testQueue",
	uint32(10),
	schedulingInfo,
	true,
	1,
	false,
	false,
	false,
	1)

var leasedJob = jobdb.NewJob(
	util.NewULID(),
	"testJobset",
	"testQueue",
	uint32(10),
	schedulingInfo,
	false,
	2,
	false,
	false,
	false,
	1).WithQueued(false).WithNewRun("testExecutor", "test-node", "node")

var (
	requeuedJobId = util.NewULID()
	requeuedJob   = jobdb.NewJob(
		requeuedJobId,
		"testJobset",
		"testQueue",
		uint32(10),
		schedulingInfo,
		true,
		2,
		false,
		false,
		false,
		1).WithUpdatedRun(
		jobdb.CreateRun(uuid.New(), requeuedJobId, time.Now().Unix(), "testExecutor", "test-node", false, false, true, false, true, true),
	)
)

// Test a single scheduler cycle
func TestScheduler_TestCycle(t *testing.T) {
	tests := map[string]struct {
		initialJobs                      []*jobdb.Job      // jobs in the jobdb at the start of the cycle
		jobUpdates                       []database.Job    // job updates from the database
		runUpdates                       []database.Run    // run updates from the database
		staleExecutor                    bool              // if true then the executorRepository will report the executor as stale
		fetchError                       bool              // if true then the jobRepository will throw an error
		scheduleError                    bool              // if true then the schedulingalgo will throw an error
		publishError                     bool              // if true the publisher will throw an error
		submitCheckerFailure             bool              // if true the submit checker will say the job is unschedulable
		expectedJobRunLeased             []string          // ids of jobs we expect to have produced leased messages
		expectedJobRunErrors             []string          // ids of jobs we expect to have produced jobRunErrors messages
		expectedJobErrors                []string          // ids of jobs we expect to have produced jobErrors messages
		expectedJobRunPreempted          []string          // ids of jobs we expect to have produced jobRunPreempted messages
		expectedJobCancelled             []string          // ids of jobs we expect to have  produced cancelled messages
		expectedJobReprioritised         []string          // ids of jobs we expect to have  produced reprioritised messages
		expectedQueued                   []string          // ids of jobs we expect to have  produced requeued messages
		expectedJobSucceeded             []string          // ids of jobs we expect to have  produced succeeeded messages
		expectedLeased                   []string          // ids of jobs we expected to be leased in jobdb at the end of the cycle
		expectedRequeued                 []string          // ids of jobs we expected to be requeued in jobdb at the end of the cycle
		expectedTerminal                 []string          // ids of jobs we expected to be terminal in jobdb at the end of the cycle
		expectedJobPriority              map[string]uint32 // expected priority of jobs at the end of the cycle
		expectedNodeAntiAffinities       []string          // list of nodes there is expected to be anti affinities for on job scheduling info
		expectedJobSchedulingInfoVersion int               // expected scheduling info version of jobs at the end of the cycle
		expectedQueuedVersion            int32             // expected queued version of jobs atthe end of the cycle
	}{
		"Lease a single job already in the db": {
			initialJobs:           []*jobdb.Job{queuedJob},
			expectedJobRunLeased:  []string{queuedJob.Id()},
			expectedLeased:        []string{queuedJob.Id()},
			expectedQueuedVersion: queuedJob.QueuedVersion() + 1,
		},
		"Lease a single job from an update": {
			jobUpdates: []database.Job{
				{
					JobID:                 queuedJob.Id(),
					JobSet:                "testJobSet",
					Queue:                 "testQueue",
					Queued:                true,
					QueuedVersion:         1,
					SchedulingInfo:        schedulingInfoBytes,
					SchedulingInfoVersion: int32(schedulingInfo.Version),
					Serial:                1,
				},
			},
			expectedJobRunLeased:  []string{queuedJob.Id()},
			expectedLeased:        []string{queuedJob.Id()},
			expectedQueuedVersion: queuedJob.QueuedVersion() + 1,
		},
		"Nothing leased": {
			initialJobs:           []*jobdb.Job{queuedJob},
			expectedQueued:        []string{queuedJob.Id()},
			expectedQueuedVersion: queuedJob.QueuedVersion(),
		},
		"No updates to an already leased job": {
			initialJobs:           []*jobdb.Job{leasedJob},
			expectedLeased:        []string{leasedJob.Id()},
			expectedQueuedVersion: leasedJob.QueuedVersion(),
		},
		"No updates to a requeued job already db": {
			initialJobs:           []*jobdb.Job{requeuedJob},
			expectedQueued:        []string{requeuedJob.Id()},
			expectedQueuedVersion: requeuedJob.QueuedVersion(),
		},
		"No updates to a requeued job from update": {
			jobUpdates: []database.Job{
				{
					JobID:                 requeuedJob.Id(),
					JobSet:                "testJobSet",
					Queue:                 "testQueue",
					Queued:                true,
					QueuedVersion:         2,
					SchedulingInfo:        schedulingInfoBytes,
					SchedulingInfoVersion: int32(schedulingInfo.Version),
					Serial:                1,
				},
			},
			runUpdates: []database.Run{
				{
					RunID:        requeuedJob.LatestRun().Id(),
					JobID:        requeuedJob.Id(),
					JobSet:       "testJobSet",
					Executor:     "testExecutor",
					Failed:       true,
					Returned:     true,
					RunAttempted: true,
					Serial:       1,
				},
			},
			expectedQueued:        []string{requeuedJob.Id()},
			expectedQueuedVersion: requeuedJob.QueuedVersion(),
		},
		"Lease returned and re-queued when run attempted": {
			initialJobs: []*jobdb.Job{leasedJob},
			runUpdates: []database.Run{
				{
					RunID:        leasedJob.LatestRun().Id(),
					JobID:        leasedJob.Id(),
					JobSet:       "testJobSet",
					Executor:     "testExecutor",
					Failed:       true,
					Returned:     true,
					RunAttempted: true,
					Serial:       1,
				},
			},
			expectedQueued:   []string{leasedJob.Id()},
			expectedRequeued: []string{leasedJob.Id()},
			// Should add node anti affinities for nodes of any attempted runs
			expectedNodeAntiAffinities:       []string{leasedJob.LatestRun().NodeName()},
			expectedJobSchedulingInfoVersion: 2,
			expectedQueuedVersion:            leasedJob.QueuedVersion() + 1,
		},
		"Lease returned and re-queued when run not attempted": {
			initialJobs: []*jobdb.Job{leasedJob},
			runUpdates: []database.Run{
				{
					RunID:        leasedJob.LatestRun().Id(),
					JobID:        leasedJob.Id(),
					JobSet:       "testJobSet",
					Executor:     "testExecutor",
					Failed:       true,
					Returned:     true,
					RunAttempted: false,
					Serial:       1,
				},
			},
			expectedQueued:        []string{leasedJob.Id()},
			expectedRequeued:      []string{leasedJob.Id()},
			expectedQueuedVersion: leasedJob.QueuedVersion() + 1,
		},
		// When a lease is returned and the run was attempted, a node anti affinity is added
		// If this node anti-affinity makes the job unschedulable, it should be failed
		"Lease returned and failed": {
			initialJobs: []*jobdb.Job{leasedJob},
			runUpdates: []database.Run{
				{
					RunID:        leasedJob.LatestRun().Id(),
					JobID:        leasedJob.Id(),
					JobSet:       "testJobSet",
					Executor:     "testExecutor",
					Failed:       true,
					Returned:     true,
					RunAttempted: true,
					Serial:       1,
				},
			},
			submitCheckerFailure:  true,
			expectedJobErrors:     []string{leasedJob.Id()},
			expectedTerminal:      []string{leasedJob.Id()},
			expectedQueuedVersion: leasedJob.QueuedVersion(),
		},
		"Lease returned too many times": {
			initialJobs: []*jobdb.Job{leasedJob},
			// 2 failures here so the second one should trigger a run failure
			runUpdates: []database.Run{
				{
					RunID:        leasedJob.LatestRun().Id(),
					JobID:        leasedJob.Id(),
					JobSet:       "testJobSet",
					Executor:     "testExecutor",
					Failed:       true,
					Returned:     true,
					RunAttempted: true,
					Serial:       1,
				},
				{
					RunID:        uuid.New(),
					JobID:        leasedJob.Id(),
					JobSet:       "testJobSet",
					Executor:     "testExecutor",
					Failed:       true,
					Returned:     true,
					RunAttempted: true,
					Serial:       2,
				},
			},
			expectedJobErrors:     []string{leasedJob.Id()},
			expectedTerminal:      []string{leasedJob.Id()},
			expectedQueuedVersion: leasedJob.QueuedVersion(),
		},
		"Job cancelled": {
			initialJobs: []*jobdb.Job{leasedJob},
			jobUpdates: []database.Job{
				{
					JobID:           leasedJob.Id(),
					JobSet:          "testJobSet",
					Queue:           "testQueue",
					CancelRequested: true,
					Serial:          1,
				},
			},
			expectedJobCancelled:  []string{leasedJob.Id()},
			expectedTerminal:      []string{leasedJob.Id()},
			expectedQueuedVersion: leasedJob.QueuedVersion(),
		},
		"Job reprioritised": {
			initialJobs: []*jobdb.Job{queuedJob},
			jobUpdates: []database.Job{
				{
					JobID:    queuedJob.Id(),
					JobSet:   "testJobSet",
					Queue:    "testQueue",
					Priority: 2,
					Serial:   1,
				},
			},
			expectedJobReprioritised: []string{queuedJob.Id()},
			expectedQueued:           []string{queuedJob.Id()},
			expectedJobPriority:      map[string]uint32{queuedJob.Id(): 2},
			expectedQueuedVersion:    queuedJob.QueuedVersion(),
		},
		"Lease expired": {
			initialJobs:           []*jobdb.Job{leasedJob},
			staleExecutor:         true,
			expectedJobRunErrors:  []string{leasedJob.Id()},
			expectedJobErrors:     []string{leasedJob.Id()},
			expectedTerminal:      []string{leasedJob.Id()},
			expectedQueuedVersion: leasedJob.QueuedVersion(),
		},
		"Job failed": {
			initialJobs: []*jobdb.Job{leasedJob},
			runUpdates: []database.Run{
				{
					RunID:    leasedJob.LatestRun().Id(),
					JobID:    leasedJob.Id(),
					JobSet:   "testJobSet",
					Executor: "testExecutor",
					Failed:   true,
					Serial:   1,
				},
			},
			expectedJobErrors:     []string{leasedJob.Id()},
			expectedTerminal:      []string{leasedJob.Id()},
			expectedQueuedVersion: leasedJob.QueuedVersion(),
		},
		"Job succeeded": {
			initialJobs: []*jobdb.Job{leasedJob},
			runUpdates: []database.Run{
				{
					RunID:     leasedJob.LatestRun().Id(),
					JobID:     leasedJob.Id(),
					JobSet:    "testJobSet",
					Executor:  "testExecutor",
					Succeeded: true,
					Serial:    1,
				},
			},
			expectedJobSucceeded:  []string{leasedJob.Id()},
			expectedTerminal:      []string{leasedJob.Id()},
			expectedQueuedVersion: leasedJob.QueuedVersion(),
		},
		"Job preempted": {
			initialJobs:             []*jobdb.Job{leasedJob},
			expectedJobRunPreempted: []string{leasedJob.Id()},
			expectedJobErrors:       []string{leasedJob.Id()},
			expectedJobRunErrors:    []string{leasedJob.Id()},
			expectedTerminal:        []string{leasedJob.Id()},
			expectedQueuedVersion:   leasedJob.QueuedVersion(),
		},
		"Fetch fails": {
			initialJobs:           []*jobdb.Job{leasedJob},
			fetchError:            true,
			expectedLeased:        []string{leasedJob.Id()},
			expectedQueuedVersion: leasedJob.QueuedVersion(),
		},
		"Schedule fails": {
			initialJobs:           []*jobdb.Job{leasedJob},
			scheduleError:         true,
			expectedLeased:        []string{leasedJob.Id()}, // job should still be leased as error was thrown and transaction rolled back
			expectedQueuedVersion: leasedJob.QueuedVersion(),
		},
		"Publish fails": {
			initialJobs:           []*jobdb.Job{leasedJob},
			publishError:          true,
			expectedLeased:        []string{leasedJob.Id()}, // job should still be leased as error was thrown and transaction rolled back
			expectedQueuedVersion: leasedJob.QueuedVersion(),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			clusterTimeout := 1 * time.Hour

			// Test objects
			jobRepo := &testJobRepository{
				updatedJobs: tc.jobUpdates,
				updatedRuns: tc.runUpdates,
				shouldError: tc.fetchError,
			}
			testClock := clock.NewFakeClock(time.Now())
			schedulingAlgo := &testSchedulingAlgo{
				jobsToSchedule: tc.expectedJobRunLeased,
				jobsToPreempt:  tc.expectedJobRunPreempted,
				shouldError:    tc.scheduleError,
			}
			publisher := &testPublisher{shouldError: tc.publishError}
			stringInterner, err := stringinterner.New(100)
			require.NoError(t, err)
			submitChecker := &testSubmitChecker{checkSuccess: !tc.submitCheckerFailure}

			heartbeatTime := testClock.Now()
			if tc.staleExecutor {
				heartbeatTime = heartbeatTime.Add(-2 * clusterTimeout)
			}
			clusterRepo := &testExecutorRepository{
				updateTimes: map[string]time.Time{"testExecutor": heartbeatTime},
			}
			sched, err := NewScheduler(
				jobRepo,
				clusterRepo,
				schedulingAlgo,
				NewStandaloneLeaderController(),
				publisher,
				stringInterner,
				submitChecker,
				1*time.Second,
				5*time.Second,
				clusterTimeout,
				maxNumberOfAttempts,
				nodeIdLabel,
			)
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

			// Assert that all expected events are generated and that all events are expected.
			outstandingEventsByType := map[string]map[string]bool{
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_JobRunLeased{}):     stringSet(tc.expectedJobRunLeased),
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_JobErrors{}):        stringSet(tc.expectedJobErrors),
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_JobRunErrors{}):     stringSet(tc.expectedJobRunErrors),
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_JobRunPreempted{}):  stringSet(tc.expectedJobRunPreempted),
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_CancelledJob{}):     stringSet(tc.expectedJobCancelled),
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_ReprioritisedJob{}): stringSet(tc.expectedJobReprioritised),
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_JobSucceeded{}):     stringSet(tc.expectedJobSucceeded),
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_JobRequeued{}):      stringSet(tc.expectedRequeued),
			}
			err = subtractEventsFromOutstandingEventsByType(publisher.events, outstandingEventsByType)
			require.NoError(t, err)
			for eventType, m := range outstandingEventsByType {
				assert.Empty(t, m, "%d outstanding events of type %s", len(m), eventType)
			}

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
			jobs := sched.jobDb.GetAll(sched.jobDb.ReadTxn())
			remainingLeased := stringSet(tc.expectedLeased)
			remainingQueued := stringSet(tc.expectedQueued)
			remainingTerminal := stringSet(tc.expectedTerminal)
			for _, job := range jobs {
				if job.InTerminalState() {
					_, ok := remainingTerminal[job.Id()]
					assert.True(t, ok)
					allRunsTerminal := true
					for _, run := range job.AllRuns() {
						if !run.InTerminalState() {
							allRunsTerminal = false
						}
					}
					assert.True(t, allRunsTerminal)
					delete(remainingTerminal, job.Id())
				} else if job.Queued() {
					_, ok := remainingQueued[job.Id()]
					assert.True(t, ok)
					delete(remainingQueued, job.Id())
				} else {
					_, ok := remainingLeased[job.Id()]
					assert.True(t, ok)
					delete(remainingLeased, job.Id())
				}
				if expectedPriority, ok := tc.expectedJobPriority[job.Id()]; ok {
					assert.Equal(t, job.Priority(), expectedPriority)
				}
				if len(tc.expectedNodeAntiAffinities) > 0 {
					assert.Len(t, job.JobSchedulingInfo().ObjectRequirements, 1)
					affinity := job.JobSchedulingInfo().ObjectRequirements[0].GetPodRequirements().Affinity
					assert.NotNil(t, affinity)
					expectedAffinity := createAntiAffinity(t, nodeIdLabel, tc.expectedNodeAntiAffinities)
					assert.Equal(t, expectedAffinity, affinity)
				}
				podRequirements := job.PodRequirements()
				assert.NotNil(t, podRequirements)

				expectedQueuedVersion := int32(1)
				if tc.expectedQueuedVersion != 0 {
					expectedQueuedVersion = tc.expectedQueuedVersion
				}
				assert.Equal(t, job.QueuedVersion(), expectedQueuedVersion)
				expectedSchedulingInfoVersion := 1
				if tc.expectedJobSchedulingInfoVersion != 0 {
					expectedSchedulingInfoVersion = tc.expectedJobSchedulingInfoVersion
				}
				assert.Equal(t, job.JobSchedulingInfo().Version, uint32(expectedSchedulingInfoVersion))
			}
			assert.Equal(t, 0, len(remainingLeased))
			assert.Equal(t, 0, len(remainingQueued))
			assert.Equal(t, 0, len(remainingTerminal))
			cancel()
		})
	}
}

func createAntiAffinity(t *testing.T, key string, values []string) *v1.Affinity {
	newAffinity := &v1.Affinity{}
	for _, value := range values {
		err := affinity.AddNodeAntiAffinity(newAffinity, key, value)
		assert.NoError(t, err)
	}
	return newAffinity
}

func subtractEventsFromOutstandingEventsByType(eventSequences []*armadaevents.EventSequence, outstandingEventsByType map[string]map[string]bool) error {
	for _, eventSequence := range eventSequences {
		for _, event := range eventSequence.Events {
			protoJobId, err := armadaevents.JobIdFromEvent(event)
			if err != nil {
				return err
			}
			jobId, err := armadaevents.UlidStringFromProtoUuid(protoJobId)
			if err != nil {
				return err
			}
			key := fmt.Sprintf("%T", event.Event)
			_, ok := outstandingEventsByType[key][jobId]
			if !ok {
				return errors.Errorf("received unexpected event for job %s: %v", jobId, event.Event)
			}
			delete(outstandingEventsByType[key], jobId)
		}
	}
	return nil
}

// Test running multiple scheduler cycles
func TestRun(t *testing.T) {
	// Test objects
	jobRepo := testJobRepository{numReceivedPartitions: 100}
	testClock := clock.NewFakeClock(time.Now())
	schedulingAlgo := &testSchedulingAlgo{}
	publisher := &testPublisher{}
	clusterRepo := &testExecutorRepository{}
	leaderController := NewStandaloneLeaderController()
	submitChecker := &testSubmitChecker{checkSuccess: true}
	stringInterner, err := stringinterner.New(100)
	require.NoError(t, err)

	sched, err := NewScheduler(
		&jobRepo,
		clusterRepo,
		schedulingAlgo,
		leaderController,
		publisher,
		stringInterner,
		submitChecker,
		1*time.Second,
		15*time.Second,
		1*time.Hour,
		maxNumberOfAttempts,
		nodeIdLabel)
	require.NoError(t, err)

	sched.clock = testClock

	ctx, cancel := context.WithCancel(context.Background())

	//nolint:errcheck
	go sched.Run(ctx)

	time.Sleep(1 * time.Second)

	// Function that runs a cycle and waits until it sees published messages
	fireCycle := func() {
		publisher.Reset()
		wg := sync.WaitGroup{}
		wg.Add(1)
		sched.onCycleCompleted = func() { wg.Done() }
		jobId := util.NewULID()
		jobRepo.updatedJobs = []database.Job{{JobID: jobId, Queue: "testQueue", Queued: true}}
		schedulingAlgo.jobsToSchedule = []string{jobId}
		testClock.Step(10 * time.Second)
		wg.Wait()
	}

	// fire a cycle and assert that we became leader and published
	fireCycle()
	assert.Equal(t, 1, len(publisher.events))
	assert.Equal(t, schedulingAlgo.numberOfScheduleCalls, 1)

	// invalidate our leadership: we should not publish
	leaderController.token = InvalidLeaderToken()
	fireCycle()
	assert.Equal(t, 0, len(publisher.events))
	assert.Equal(t, schedulingAlgo.numberOfScheduleCalls, 1)

	// become master again: we should publish
	leaderController.token = NewLeaderToken()
	fireCycle()
	assert.Equal(t, 1, len(publisher.events))
	assert.Equal(t, schedulingAlgo.numberOfScheduleCalls, 2)

	cancel()
}

func TestScheduler_TestSyncState(t *testing.T) {
	tests := map[string]struct {
		initialJobs         []*jobdb.Job   // jobs in the jobdb at the start of the cycle
		jobUpdates          []database.Job // job updates from the database
		runUpdates          []database.Run // run updates from the database
		expectedUpdatedJobs []*jobdb.Job
		expectedJobDbIds    []string
	}{
		"insert job": {
			jobUpdates: []database.Job{
				{
					JobID:          queuedJob.Id(),
					JobSet:         queuedJob.Jobset(),
					Queue:          queuedJob.Queue(),
					Submitted:      queuedJob.Created(),
					Queued:         true,
					QueuedVersion:  1,
					Priority:       int64(queuedJob.Priority()),
					SchedulingInfo: schedulingInfoBytes,
					Serial:         1,
				},
			},
			expectedUpdatedJobs: []*jobdb.Job{queuedJob},
			expectedJobDbIds:    []string{queuedJob.Id()},
		},
		"insert job that already exists": {
			initialJobs: []*jobdb.Job{queuedJob},
			jobUpdates: []database.Job{
				{
					JobID:          queuedJob.Id(),
					JobSet:         queuedJob.Jobset(),
					Queue:          queuedJob.Queue(),
					Submitted:      queuedJob.Created(),
					Priority:       int64(queuedJob.Priority()),
					SchedulingInfo: schedulingInfoBytes,
					Serial:         1,
				},
			},
			expectedUpdatedJobs: []*jobdb.Job{queuedJob},
			expectedJobDbIds:    []string{queuedJob.Id()},
		},
		"add job run": {
			initialJobs: []*jobdb.Job{queuedJob},
			jobUpdates: []database.Job{
				{
					JobID:          queuedJob.Id(),
					JobSet:         queuedJob.Jobset(),
					Queue:          queuedJob.Queue(),
					Queued:         false,
					QueuedVersion:  2,
					Priority:       int64(queuedJob.Priority()),
					SchedulingInfo: schedulingInfoBytes,
					Serial:         2,
				},
			},
			runUpdates: []database.Run{
				{
					RunID:    uuid.UUID{},
					JobID:    queuedJob.Id(),
					JobSet:   queuedJob.Jobset(),
					Executor: "test-executor",
					Node:     "test-node",
					Created:  123,
				},
			},
			expectedUpdatedJobs: []*jobdb.Job{
				queuedJob.WithUpdatedRun(
					jobdb.CreateRun(
						uuid.UUID{},
						queuedJob.Id(),
						123,
						"test-executor",
						"test-node",
						false,
						false,
						false,
						false,
						false,
						false,
					),
				).WithQueued(false).WithQueuedVersion(2),
			},
			expectedJobDbIds: []string{queuedJob.Id()},
		},
		"job succeeded": {
			initialJobs: []*jobdb.Job{queuedJob},
			jobUpdates: []database.Job{
				{
					JobID:          queuedJob.Id(),
					JobSet:         queuedJob.Jobset(),
					Queue:          queuedJob.Queue(),
					Submitted:      queuedJob.Created(),
					Priority:       int64(queuedJob.Priority()),
					SchedulingInfo: schedulingInfoBytes,
					Succeeded:      true,
					Serial:         1,
				},
			},
			expectedUpdatedJobs: []*jobdb.Job{},
			expectedJobDbIds:    []string{},
		},
		"job requeued": {
			initialJobs: []*jobdb.Job{leasedJob},
			jobUpdates: []database.Job{
				{
					JobID:                 leasedJob.Id(),
					JobSet:                leasedJob.Jobset(),
					Queue:                 leasedJob.Queue(),
					Submitted:             leasedJob.Created(),
					Queued:                true,
					QueuedVersion:         3,
					Priority:              int64(leasedJob.Priority()),
					SchedulingInfo:        updatedSchedulingInfoBytes,
					SchedulingInfoVersion: int32(updatedSchedulingInfo.Version),
					Serial:                1,
				},
			},
			expectedUpdatedJobs: []*jobdb.Job{
				leasedJob.
					WithJobSchedulingInfo(updatedSchedulingInfo).
					WithQueued(true).
					WithQueuedVersion(3),
			},
			expectedJobDbIds: []string{leasedJob.Id()},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Test objects
			jobRepo := &testJobRepository{
				updatedJobs: tc.jobUpdates,
				updatedRuns: tc.runUpdates,
			}
			schedulingAlgo := &testSchedulingAlgo{}
			publisher := &testPublisher{}
			clusterRepo := &testExecutorRepository{}
			leaderController := NewStandaloneLeaderController()
			stringInterner, err := stringinterner.New(100)
			require.NoError(t, err)

			sched, err := NewScheduler(
				jobRepo,
				clusterRepo,
				schedulingAlgo,
				leaderController,
				publisher,
				stringInterner,
				nil,
				1*time.Second,
				5*time.Second,
				1*time.Hour,
				maxNumberOfAttempts,
				nodeIdLabel)
			require.NoError(t, err)

			// insert initial jobs
			txn := sched.jobDb.WriteTxn()
			err = sched.jobDb.Upsert(txn, tc.initialJobs)
			require.NoError(t, err)
			txn.Commit()

			updatedJobs, err := sched.syncState(ctx)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedUpdatedJobs, updatedJobs)
			allDbJobs := sched.jobDb.GetAll(sched.jobDb.ReadTxn())

			expectedIds := stringSet(tc.expectedJobDbIds)
			require.Equal(t, len(tc.expectedJobDbIds), len(allDbJobs))
			for _, job := range allDbJobs {
				_, ok := expectedIds[job.Id()]
				assert.True(t, ok)
			}
		})
	}
}

type testSubmitChecker struct {
	checkSuccess bool
}

func (t *testSubmitChecker) CheckApiJobs(_ []*api.Job) (bool, string) {
	reason := ""
	if !t.checkSuccess {
		reason = "CheckApiJobs failed"
	}
	return t.checkSuccess, reason
}

func (t *testSubmitChecker) CheckJobDbJobs(_ []*jobdb.Job) (bool, string) {
	reason := ""
	if !t.checkSuccess {
		reason = "CheckJobDbJobs failed"
	}
	return t.checkSuccess, reason
}

// Test implementations of the interfaces needed by the Scheduler
type testJobRepository struct {
	updatedJobs           []database.Job
	updatedRuns           []database.Run
	errors                map[uuid.UUID]*armadaevents.Error
	shouldError           bool
	numReceivedPartitions uint32
}

func (t *testJobRepository) FindInactiveRuns(ctx context.Context, runIds []uuid.UUID) ([]uuid.UUID, error) {
	// TODO implement me
	panic("implement me")
}

func (t *testJobRepository) FetchJobRunLeases(ctx context.Context, executor string, maxResults uint, excludedRunIds []uuid.UUID) ([]*database.JobRunLease, error) {
	// TODO implement me
	panic("implement me")
}

func (t *testJobRepository) FetchJobUpdates(ctx context.Context, jobSerial int64, jobRunSerial int64) ([]database.Job, []database.Run, error) {
	if t.shouldError {
		return nil, nil, errors.New("error fetchiung job updates")
	}
	return t.updatedJobs, t.updatedRuns, nil
}

func (t *testJobRepository) FetchJobRunErrors(ctx context.Context, runIds []uuid.UUID) (map[uuid.UUID]*armadaevents.Error, error) {
	if t.shouldError {
		return nil, errors.New("error fetching job run errors")
	}
	return t.errors, nil
}

func (t *testJobRepository) CountReceivedPartitions(ctx context.Context, groupId uuid.UUID) (uint32, error) {
	if t.shouldError {
		return 0, errors.New("error counting received partitions")
	}
	return t.numReceivedPartitions, nil
}

type testExecutorRepository struct {
	updateTimes map[string]time.Time
	shouldError bool
}

func (t testExecutorRepository) GetExecutors(ctx context.Context) ([]*schedulerobjects.Executor, error) {
	panic("not implemented")
}

func (t testExecutorRepository) GetLastUpdateTimes(ctx context.Context) (map[string]time.Time, error) {
	if t.shouldError {
		return nil, errors.New("error getting last update time")
	}
	return t.updateTimes, nil
}

func (t testExecutorRepository) StoreExecutor(ctx context.Context, executor *schedulerobjects.Executor) error {
	panic("not implemented")
}

type testSchedulingAlgo struct {
	numberOfScheduleCalls int
	jobsToPreempt         []string
	jobsToSchedule        []string
	shouldError           bool
}

func (t *testSchedulingAlgo) Schedule(ctx context.Context, txn *jobdb.Txn, jobDb *jobdb.JobDb) (*SchedulerResult, error) {
	t.numberOfScheduleCalls++
	if t.shouldError {
		return nil, errors.New("error scheduling jobs")
	}
	preemptedJobs := make([]*jobdb.Job, 0, len(t.jobsToPreempt))
	scheduledJobs := make([]*jobdb.Job, 0, len(t.jobsToSchedule))
	for _, id := range t.jobsToPreempt {
		job := jobDb.GetById(txn, id)
		if job == nil {
			return nil, errors.Errorf("was asked to preempt job %s but job does not exist", id)
		}
		if job.Queued() {
			return nil, errors.Errorf("was asked to preempt job %s but job is still queued", job.Id())
		}
		if run := job.LatestRun(); run != nil {
			job = job.WithUpdatedRun(run.WithFailed(true))
		} else {
			return nil, errors.Errorf("attempting to preempt job %s with no associated runs", job.Id())
		}
		job = job.WithQueued(false).WithFailed(true)
		preemptedJobs = append(preemptedJobs, job)
	}
	for _, id := range t.jobsToSchedule {
		job := jobDb.GetById(txn, id)
		if job == nil {
			return nil, errors.Errorf("was asked to lease %s but job does not exist", id)
		}
		if !job.Queued() {
			return nil, errors.Errorf("was asked to lease %s but job was already leased", job.Id())
		}
		job = job.WithQueued(false).WithNewRun("test-executor", "test-node", "node")
		scheduledJobs = append(scheduledJobs, job)
	}
	if err := jobDb.Upsert(txn, preemptedJobs); err != nil {
		return nil, err
	}
	if err := jobDb.Upsert(txn, scheduledJobs); err != nil {
		return nil, err
	}
	return NewSchedulerResult(preemptedJobs, scheduledJobs, nil), nil
}

type testPublisher struct {
	events      []*armadaevents.EventSequence
	shouldError bool
}

func (t *testPublisher) PublishMessages(ctx context.Context, events []*armadaevents.EventSequence, _ func() bool) error {
	t.events = events
	if t.shouldError {
		return errors.New("Error when publishing")
	}
	return nil
}

func (t *testPublisher) Reset() {
	t.events = nil
}

func (t *testPublisher) PublishMarkers(ctx context.Context, groupId uuid.UUID) (uint32, error) {
	return 100, nil
}

func stringSet(src []string) map[string]bool {
	set := make(map[string]bool, len(src))
	for _, s := range src {
		set[s] = true
	}
	return set
}
