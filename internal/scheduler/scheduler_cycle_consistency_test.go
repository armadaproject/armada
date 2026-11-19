//go:build !no_postgres

package scheduler

import (
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	clock "k8s.io/utils/clock/testing"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/errormatch"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/leaderelection"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/database"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/pricing"
	"github.com/armadaproject/armada/internal/scheduler/publisher"
	"github.com/armadaproject/armada/internal/scheduler/retry"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/runner"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/internal/scheduleringester"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

var (
	queuedJobA = &database.Job{
		JobID:                 util.NewULID(),
		JobSet:                "testJobSet",
		Queue:                 "testQueue",
		Queued:                true,
		QueuedVersion:         0,
		SchedulingInfo:        schedulingInfoBytes,
		SchedulingInfoVersion: int32(schedulingInfo.Version),
		Validated:             true,
		Serial:                0,
	}
	queuedJobWithFailFastA = &database.Job{
		JobID:                 queuedJobA.JobID,
		JobSet:                "testJobSet",
		Queue:                 "testQueue",
		Queued:                true,
		QueuedVersion:         0,
		SchedulingInfo:        failFastSchedulingInfoBytes,
		SchedulingInfoVersion: int32(failFastSchedulingInfo.Version),
		Validated:             true,
		Serial:                0,
	}
	queuedJobWithUpdatedPriorityA = &database.Job{
		JobID:                 queuedJobA.JobID,
		JobSet:                "testJobSet",
		Queue:                 "testQueue",
		Queued:                true,
		QueuedVersion:         0,
		SchedulingInfo:        schedulingInfoWithUpdatedPriorityBytes,
		SchedulingInfoVersion: int32(schedulingInfoWithUpdatedPriority.Version),
		Validated:             true,
		Serial:                1,
	}
	runningJobA = &database.Job{
		JobID:                 queuedJobA.JobID,
		JobSet:                "testJobSet",
		Queue:                 "testQueue",
		QueuedVersion:         1,
		SchedulingInfo:        schedulingInfoBytes,
		SchedulingInfoVersion: int32(schedulingInfo.Version),
		Validated:             true,
		Serial:                0,
	}
	failedJobA = &database.Job{
		JobID:                 queuedJobA.JobID,
		JobSet:                "testJobSet",
		Queue:                 "testQueue",
		Failed:                true,
		QueuedVersion:         0,
		SchedulingInfo:        schedulingInfoBytes,
		SchedulingInfoVersion: int32(schedulingInfo.Version),
		Validated:             true,
		Serial:                0,
	}
	cancelledJobA = &database.Job{
		JobID:                 queuedJobA.JobID,
		JobSet:                "testJobSet",
		Queue:                 "testQueue",
		Cancelled:             true,
		QueuedVersion:         0,
		SchedulingInfo:        schedulingInfoBytes,
		SchedulingInfoVersion: int32(schedulingInfo.Version),
		Validated:             true,
		Serial:                0,
	}
	cancelRequestedJobA = &database.Job{
		JobID:                 queuedJobA.JobID,
		JobSet:                "testJobSet",
		Queue:                 "testQueue",
		Queued:                true,
		CancelRequested:       true,
		CancelUser:            pointer.String("cancel-user-a"),
		QueuedVersion:         0,
		SchedulingInfo:        schedulingInfoBytes,
		SchedulingInfoVersion: int32(schedulingInfo.Version),
		Validated:             true,
		Serial:                0,
	}
	cancelByJobSetRequestedJobA = &database.Job{
		JobID:                   queuedJobA.JobID,
		JobSet:                  "testJobSet",
		Queue:                   "testQueue",
		Queued:                  true,
		CancelByJobsetRequested: true,
		CancelUser:              pointer.String("cancel-user-a"),
		QueuedVersion:           0,
		SchedulingInfo:          schedulingInfoBytes,
		SchedulingInfoVersion:   int32(schedulingInfo.Version),
		Validated:               true,
		Serial:                  0,
	}
	runningCancelRequestedJobA = &database.Job{
		JobID:                 queuedJobA.JobID,
		JobSet:                "testJobSet",
		Queue:                 "testQueue",
		Queued:                false,
		CancelRequested:       true,
		CancelUser:            pointer.String("cancel-user-a"),
		QueuedVersion:         1,
		SchedulingInfo:        schedulingInfoBytes,
		SchedulingInfoVersion: int32(schedulingInfo.Version),
		Validated:             true,
		Serial:                0,
	}
	runningCancelByJobSetRequestedJobA = &database.Job{
		JobID:                   queuedJobA.JobID,
		JobSet:                  "testJobSet",
		Queue:                   "testQueue",
		CancelByJobsetRequested: true,
		CancelUser:              pointer.String("cancel-user-a"),
		QueuedVersion:           1,
		SchedulingInfo:          schedulingInfoBytes,
		SchedulingInfoVersion:   int32(schedulingInfo.Version),
		Validated:               true,
		Serial:                  0,
	}
	newRunA = &database.Run{
		RunID:               testfixtures.UUIDFromInt(1).String(),
		JobID:               queuedJobA.JobID,
		JobSet:              queuedJobA.JobSet,
		Executor:            testExecutor,
		Node:                testNode,
		RunAttempted:        true,
		Serial:              0,
		ScheduledAtPriority: &scheduledAtPriority,
	}
	successfulRunA = &database.Run{
		RunID:               testfixtures.UUIDFromInt(1).String(),
		JobID:               queuedJobA.JobID,
		JobSet:              queuedJobA.JobSet,
		Executor:            testExecutor,
		Node:                testNode,
		Succeeded:           true,
		RunAttempted:        true,
		Serial:              0,
		ScheduledAtPriority: &scheduledAtPriority,
	}
	failedAttemptedRunA = &database.Run{
		RunID:               testfixtures.UUIDFromInt(1).String(),
		JobID:               queuedJobA.JobID,
		JobSet:              queuedJobA.JobSet,
		Executor:            testExecutor,
		Node:                testNode,
		Failed:              true,
		RunAttempted:        true,
		Serial:              0,
		ScheduledAtPriority: &scheduledAtPriority,
	}
	failedRunA = &database.Run{
		RunID:               testfixtures.UUIDFromInt(1).String(),
		JobID:               queuedJobA.JobID,
		JobSet:              queuedJobA.JobSet,
		Executor:            testExecutor,
		Node:                testNode,
		Failed:              true,
		Serial:              0,
		ScheduledAtPriority: &scheduledAtPriority,
	}
	failedReturnedRunA = &database.Run{
		RunID:               testfixtures.UUIDFromInt(1).String(),
		JobID:               queuedJobA.JobID,
		JobSet:              queuedJobA.JobSet,
		Executor:            testExecutor,
		Node:                testNode,
		Failed:              true,
		Returned:            true,
		Serial:              0,
		ScheduledAtPriority: &scheduledAtPriority,
	}
)

func jobDbJobFromDbJob(resourceListFactory *internaltypes.ResourceListFactory, job *database.Job) *jobdb.Job {
	var schedulingInfo schedulerobjects.JobSchedulingInfo
	protoutil.MustUnmarshall(job.SchedulingInfo, &schedulingInfo)
	// Use a fresh jobDb instance to ensure run ids are consistent.
	result, err := testfixtures.NewJobDb(resourceListFactory).NewJob(
		job.JobID,
		job.JobSet,
		job.Queue,
		uint32(job.Priority),
		toInternalSchedulingInfo(&schedulingInfo),
		job.Queued,
		job.QueuedVersion,
		job.CancelRequested,
		job.CancelByJobsetRequested,
		job.Cancelled,
		0,
		job.Validated,
		job.Pools,
		0,
	)
	if err != nil {
		panic(err)
	}
	if job.CancelUser != nil {
		result = result.WithCancelUser(job.CancelUser)
	}
	if job.CancelReason != nil {
		result = result.WithCancelReason(job.CancelReason)
	}
	if job.ReprioritiseUser != nil {
		result = result.WithReprioritiseUser(job.ReprioritiseUser)
	}
	return result
}

// TestCycleConsistency runs two replicas of the scheduler and asserts that their state remains consistent
// under various permutations of making scheduling decisions and failing over.
//
// TODO(albin): Test lease expiry.
func TestCycleConsistency(t *testing.T) {
	resourceListFactory := testfixtures.MakeTestResourceListFactory()
	type schedulerDbUpdate struct {
		jobUpdates   []*database.Job              // Job updates from the database.
		runUpdates   []*database.Run              // Run updates from the database.
		jobRunErrors []*armadaevents.JobRunErrors // Job run errors from the database.
	}
	tests := map[string]struct {
		// Each test case consists of two updates written to the scheduler postgres.
		firstSchedulerDbUpdate  schedulerDbUpdate
		secondSchedulerDbUpdate schedulerDbUpdate

		// Inputs of the mocked scheduling algorithm.
		// Controls which jobs the scheduler should schedule/preempt/fail.
		idsOfJobsToSchedule []string
		idsOfJobsToPreempt  []string

		// Expected jobDbs for scenario 1, i.e., the baseline scenario.
		// Only compared against if not nil.
		expectedJobDbCycleOne   []*jobdb.Job
		expectedJobDbCycleTwo   []*jobdb.Job
		expectedJobDbCycleThree []*jobdb.Job

		// Expected published events for scenario 1, i.e., the baseline scenario.
		// Only compared against if not nil.
		expectedEventSequencesCycleOne   []*armadaevents.EventSequence
		expectedEventSequencesCycleTwo   []*armadaevents.EventSequence
		expectedEventSequencesCycleThree []*armadaevents.EventSequence

		// If true then will jobs will fail submit check
		failSubmitCheck bool
	}{
		"Load a queued job": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					queuedJobA,
				},
			},
			expectedJobDbCycleOne: []*jobdb.Job{
				jobDbJobFromDbJob(resourceListFactory, queuedJobA),
			},
			expectedJobDbCycleTwo: []*jobdb.Job{
				jobDbJobFromDbJob(resourceListFactory, queuedJobA),
			},
			expectedJobDbCycleThree: []*jobdb.Job{
				jobDbJobFromDbJob(resourceListFactory, queuedJobA),
			},
			expectedEventSequencesCycleThree: make([]*armadaevents.EventSequence, 0),
		},
		"Load a failed job": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					failedJobA,
				},
			},
			expectedJobDbCycleOne:            []*jobdb.Job{},
			expectedJobDbCycleTwo:            []*jobdb.Job{},
			expectedJobDbCycleThree:          []*jobdb.Job{},
			expectedEventSequencesCycleThree: make([]*armadaevents.EventSequence, 0),
		},
		"Load a cancelled job": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					cancelledJobA,
				},
			},
			expectedJobDbCycleOne:            []*jobdb.Job{},
			expectedJobDbCycleTwo:            []*jobdb.Job{},
			expectedJobDbCycleThree:          []*jobdb.Job{},
			expectedEventSequencesCycleThree: make([]*armadaevents.EventSequence, 0),
		},
		"Load a queued job with cancel requested": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					cancelRequestedJobA,
				},
			},
			expectedJobDbCycleOne: []*jobdb.Job{
				jobDbJobFromDbJob(resourceListFactory, cancelRequestedJobA).WithQueued(false).WithCancelled(true),
			},
			expectedJobDbCycleTwo:   []*jobdb.Job{},
			expectedJobDbCycleThree: []*jobdb.Job{},
			expectedEventSequencesCycleThree: []*armadaevents.EventSequence{
				{
					Queue:      queuedJobA.Queue,
					JobSetName: queuedJobA.JobSet,
					Events: []*armadaevents.EventSequence_Event{
						{
							Created: &types.Timestamp{},
							Event: &armadaevents.EventSequence_Event_CancelledJob{
								CancelledJob: &armadaevents.CancelledJob{
									JobId:     queuedJobA.JobID,
									Requestor: "cancel-user-a",
								},
							},
						},
					},
				},
			},
		},
		"Load a queued job with cancel by job set requested": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					cancelByJobSetRequestedJobA,
				},
			},
			expectedJobDbCycleOne: []*jobdb.Job{
				jobDbJobFromDbJob(resourceListFactory, cancelByJobSetRequestedJobA).WithQueued(false).WithCancelled(true),
			},
			expectedJobDbCycleTwo:   []*jobdb.Job{},
			expectedJobDbCycleThree: []*jobdb.Job{},
			expectedEventSequencesCycleThree: []*armadaevents.EventSequence{
				{
					Queue:      queuedJobA.Queue,
					JobSetName: queuedJobA.JobSet,
					Events: []*armadaevents.EventSequence_Event{
						{
							Created: &types.Timestamp{},
							Event: &armadaevents.EventSequence_Event_CancelJob{
								CancelJob: &armadaevents.CancelJob{
									JobId:     queuedJobA.JobID,
									Requestor: "cancel-user-a",
								},
							},
						},
						{
							Created: &types.Timestamp{},
							Event: &armadaevents.EventSequence_Event_CancelledJob{
								CancelledJob: &armadaevents.CancelledJob{
									JobId:     queuedJobA.JobID,
									Requestor: "cancel-user-a",
								},
							},
						},
					},
				},
			},
		},
		"Load a running job with cancel requested": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					runningCancelRequestedJobA,
				},
				runUpdates: []*database.Run{
					newRunA,
				},
			},
			expectedJobDbCycleOne: []*jobdb.Job{
				func() *jobdb.Job {
					job := jobDbJobFromDbJob(resourceListFactory, runningCancelRequestedJobA).WithCancelled(true).WithNewRun(testExecutor, testNodeId, testNode, testPool, 2)
					return job.WithUpdatedRun(job.LatestRun().WithCancelled(true).WithAttempted(true))
				}(),
			},
			expectedJobDbCycleTwo:   []*jobdb.Job{},
			expectedJobDbCycleThree: []*jobdb.Job{},
			expectedEventSequencesCycleThree: []*armadaevents.EventSequence{
				{
					Queue:      queuedJobA.Queue,
					JobSetName: queuedJobA.JobSet,
					Events: []*armadaevents.EventSequence_Event{
						{
							Created: &types.Timestamp{},
							Event: &armadaevents.EventSequence_Event_JobRunCancelled{
								JobRunCancelled: &armadaevents.JobRunCancelled{
									RunId:     testfixtures.UUIDFromInt(1).String(),
									JobId:     queuedJobA.JobID,
									Requestor: "cancel-user-a",
								},
							},
						},
						{
							Created: &types.Timestamp{},
							Event: &armadaevents.EventSequence_Event_CancelledJob{
								CancelledJob: &armadaevents.CancelledJob{
									JobId:     queuedJobA.JobID,
									Requestor: "cancel-user-a",
								},
							},
						},
					},
				},
			},
		},
		"Load a running job with cancel by job set requested": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					runningCancelByJobSetRequestedJobA,
				},
				runUpdates: []*database.Run{
					newRunA,
				},
			},
			expectedJobDbCycleOne: []*jobdb.Job{
				func() *jobdb.Job {
					job := jobDbJobFromDbJob(resourceListFactory, runningCancelByJobSetRequestedJobA).WithCancelled(true).WithNewRun(testExecutor, testNodeId, testNode, testPool, 2)
					return job.WithUpdatedRun(job.LatestRun().WithCancelled(true).WithAttempted(true))
				}(),
			},
			expectedJobDbCycleTwo:   []*jobdb.Job{},
			expectedJobDbCycleThree: []*jobdb.Job{},
			expectedEventSequencesCycleThree: []*armadaevents.EventSequence{
				{
					Queue:      queuedJobA.Queue,
					JobSetName: queuedJobA.JobSet,
					Events: []*armadaevents.EventSequence_Event{
						{
							Created: &types.Timestamp{},
							Event: &armadaevents.EventSequence_Event_CancelJob{
								CancelJob: &armadaevents.CancelJob{
									JobId:     queuedJobA.JobID,
									Requestor: "cancel-user-a",
								},
							},
						},
						{
							Created: &types.Timestamp{},
							Event: &armadaevents.EventSequence_Event_JobRunCancelled{
								JobRunCancelled: &armadaevents.JobRunCancelled{
									JobId:     queuedJobA.JobID,
									RunId:     testfixtures.UUIDFromInt(1).String(),
									Requestor: "cancel-user-a",
								},
							},
						},
						{
							Created: &types.Timestamp{},
							Event: &armadaevents.EventSequence_Event_CancelledJob{
								CancelledJob: &armadaevents.CancelledJob{
									JobId:     queuedJobA.JobID,
									Requestor: "cancel-user-a",
								},
							},
						},
					},
				},
			},
		},
		"Schedule a new job": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					queuedJobA,
				},
			},
			idsOfJobsToSchedule: []string{queuedJobA.JobID},
			expectedJobDbCycleOne: []*jobdb.Job{
				jobDbJobFromDbJob(resourceListFactory, queuedJobA).WithQueued(false).WithQueuedVersion(1).WithNewRun(testExecutor, testNodeId, testNode, testPool, 2),
			},
			expectedJobDbCycleTwo: []*jobdb.Job{
				jobDbJobFromDbJob(resourceListFactory, queuedJobA).WithQueued(false).WithQueuedVersion(1).WithNewRun(testExecutor, testNodeId, testNode, testPool, 2),
			},
			expectedJobDbCycleThree: []*jobdb.Job{
				jobDbJobFromDbJob(resourceListFactory, queuedJobA).WithQueued(false).WithQueuedVersion(1).WithNewRun(testExecutor, testNodeId, testNode, testPool, 2),
			},
			expectedEventSequencesCycleThree: []*armadaevents.EventSequence{
				{
					Queue:      queuedJobA.Queue,
					JobSetName: queuedJobA.JobSet,
					Events: []*armadaevents.EventSequence_Event{
						{
							Created: &types.Timestamp{},
							Event: &armadaevents.EventSequence_Event_JobRunLeased{
								JobRunLeased: &armadaevents.JobRunLeased{
									RunId:                  testfixtures.UUIDFromInt(1).String(),
									JobId:                  queuedJobA.JobID,
									ExecutorId:             testExecutor,
									NodeId:                 testNode,
									UpdateSequenceNumber:   1,
									HasScheduledAtPriority: true,
									ScheduledAtPriority:    2,
									PodRequirementsOverlay: &schedulerobjects.PodRequirements{},
									Pool:                   testPool,
								},
							},
						},
					},
				},
			},
		},
		"Schedule a job that then succeeds": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					queuedJobA,
				},
			},
			secondSchedulerDbUpdate: schedulerDbUpdate{
				runUpdates: []*database.Run{
					successfulRunA,
				},
			},
			idsOfJobsToSchedule: []string{queuedJobA.JobID},
			expectedJobDbCycleOne: []*jobdb.Job{
				jobDbJobFromDbJob(resourceListFactory, queuedJobA).WithQueued(false).WithQueuedVersion(1).WithNewRun(testExecutor, testNodeId, testNode, testPool, 2),
			},
			expectedJobDbCycleTwo: []*jobdb.Job{
				func() *jobdb.Job {
					job := jobDbJobFromDbJob(resourceListFactory, queuedJobA).WithQueued(false).WithQueuedVersion(1).WithNewRun(testExecutor, testNodeId, testNode, testPool, 2).WithSucceeded(true)
					return job.WithUpdatedRun(job.LatestRun().WithSucceeded(true).WithAttempted(true))
				}(),
			},
			expectedJobDbCycleThree: make([]*jobdb.Job, 0),
			expectedEventSequencesCycleThree: []*armadaevents.EventSequence{
				{
					Queue:      queuedJobA.Queue,
					JobSetName: queuedJobA.JobSet,
					Events: []*armadaevents.EventSequence_Event{
						{
							Created: &types.Timestamp{},
							Event: &armadaevents.EventSequence_Event_JobRunLeased{
								JobRunLeased: &armadaevents.JobRunLeased{
									JobId:                  queuedJobA.JobID,
									RunId:                  testfixtures.UUIDFromInt(1).String(),
									ExecutorId:             testExecutor,
									NodeId:                 testNode,
									UpdateSequenceNumber:   1,
									HasScheduledAtPriority: true,
									ScheduledAtPriority:    2,
									PodRequirementsOverlay: &schedulerobjects.PodRequirements{},
									Pool:                   testPool,
								},
							},
						},
					},
				},
				{
					Queue:      queuedJobA.Queue,
					JobSetName: queuedJobA.JobSet,
					Events: []*armadaevents.EventSequence_Event{
						{
							Created: &types.Timestamp{},
							Event: &armadaevents.EventSequence_Event_JobSucceeded{
								JobSucceeded: &armadaevents.JobSucceeded{
									JobId: queuedJobA.JobID,
								},
							},
						},
					},
				},
			},
		},
		"Schedule a new job that then fails after starting to run": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					queuedJobA,
				},
			},
			secondSchedulerDbUpdate: schedulerDbUpdate{
				runUpdates: []*database.Run{
					failedAttemptedRunA,
				},
				jobRunErrors: []*armadaevents.JobRunErrors{
					defaultJobRunError(queuedJobA.JobID, failedAttemptedRunA.RunID),
				},
			},
			idsOfJobsToSchedule: []string{queuedJobA.JobID},
			expectedJobDbCycleOne: []*jobdb.Job{
				jobDbJobFromDbJob(resourceListFactory, queuedJobA).WithQueued(false).WithQueuedVersion(1).WithNewRun(testExecutor, testNodeId, testNode, testPool, 2),
			},
			expectedJobDbCycleTwo: []*jobdb.Job{
				func() *jobdb.Job {
					job := jobDbJobFromDbJob(resourceListFactory, queuedJobA).WithQueued(false).WithQueuedVersion(1).WithNewRun(testExecutor, testNodeId, testNode, testPool, 2).WithFailed(true)
					return job.WithUpdatedRun(job.LatestRun().WithFailed(true).WithAttempted(true))
				}(),
			},
			expectedJobDbCycleThree: make([]*jobdb.Job, 0),
			expectedEventSequencesCycleThree: []*armadaevents.EventSequence{
				{
					Queue:      queuedJobA.Queue,
					JobSetName: queuedJobA.JobSet,
					Events: []*armadaevents.EventSequence_Event{
						{
							Created: &types.Timestamp{},
							Event: &armadaevents.EventSequence_Event_JobRunLeased{
								JobRunLeased: &armadaevents.JobRunLeased{
									JobId:                  queuedJobA.JobID,
									RunId:                  testfixtures.UUIDFromInt(1).String(),
									ExecutorId:             testExecutor,
									NodeId:                 testNode,
									UpdateSequenceNumber:   1,
									HasScheduledAtPriority: true,
									ScheduledAtPriority:    2,
									PodRequirementsOverlay: &schedulerobjects.PodRequirements{},
									Pool:                   testPool,
								},
							},
						},
					},
				},
				{
					Queue:      queuedJobA.Queue,
					JobSetName: queuedJobA.JobSet,
					Events: []*armadaevents.EventSequence_Event{
						{
							Created: &types.Timestamp{},
							Event: &armadaevents.EventSequence_Event_JobErrors{
								JobErrors: &armadaevents.JobErrors{
									JobId: queuedJobA.JobID,
									Errors: []*armadaevents.Error{
										{
											Terminal: true,
											Reason: &armadaevents.Error_PodError{
												PodError: &armadaevents.PodError{
													Message: "generic pod error",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		"Schedule a new job that then fails to start": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					queuedJobA,
				},
			},
			secondSchedulerDbUpdate: schedulerDbUpdate{
				runUpdates: []*database.Run{
					failedRunA,
				},
				jobRunErrors: []*armadaevents.JobRunErrors{
					defaultJobRunError(queuedJobA.JobID, failedAttemptedRunA.RunID),
				},
			},
			idsOfJobsToSchedule: []string{queuedJobA.JobID},
			expectedJobDbCycleOne: []*jobdb.Job{
				jobDbJobFromDbJob(resourceListFactory, queuedJobA).WithQueued(false).WithQueuedVersion(1).WithNewRun(testExecutor, testNodeId, testNode, testPool, 2),
			},
			expectedJobDbCycleTwo: []*jobdb.Job{
				func() *jobdb.Job {
					job := jobDbJobFromDbJob(resourceListFactory, queuedJobA).WithQueued(false).WithQueuedVersion(1).WithNewRun(testExecutor, testNodeId, testNode, testPool, 2).WithFailed(true)
					// TODO(albin): RunAttempted is implicitly set to true for failed runs with error other than PodLeaseReturned.
					//              See func (c *JobSetEventsInstructionConverter) handleJobRunErrors.
					return job.WithUpdatedRun(job.LatestRun().WithFailed(true).WithAttempted(true))
				}(),
			},
			expectedJobDbCycleThree: []*jobdb.Job{},
		},
		"Schedule a new job that then fails to start and is returned": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					queuedJobA,
				},
			},
			secondSchedulerDbUpdate: schedulerDbUpdate{
				runUpdates: []*database.Run{
					failedReturnedRunA,
				},
				jobRunErrors: []*armadaevents.JobRunErrors{
					defaultJobRunError(queuedJobA.JobID, failedAttemptedRunA.RunID),
				},
			},
			idsOfJobsToSchedule: []string{queuedJobA.JobID},
			expectedJobDbCycleOne: []*jobdb.Job{
				jobDbJobFromDbJob(resourceListFactory, queuedJobA).WithQueued(false).WithQueuedVersion(1).WithNewRun(testExecutor, testNodeId, testNode, testPool, 2),
			},
			expectedJobDbCycleTwo: []*jobdb.Job{
				func() *jobdb.Job {
					job := jobDbJobFromDbJob(resourceListFactory, queuedJobA).WithQueued(false).WithQueuedVersion(1).WithNewRun(testExecutor, testNodeId, testNode, testPool, 2).WithFailed(true)
					return job.WithUpdatedRun(job.LatestRun().WithFailed(true).WithAttempted(true).WithReturned(true))
				}(),
			},
			expectedJobDbCycleThree: []*jobdb.Job{},
			failSubmitCheck:         true,
		},
		"Schedule a new job that then fails to start with fail-fast set to true": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					queuedJobWithFailFastA,
				},
			},
			secondSchedulerDbUpdate: schedulerDbUpdate{
				runUpdates: []*database.Run{
					failedRunA,
				},
				jobRunErrors: []*armadaevents.JobRunErrors{
					defaultJobRunError(queuedJobA.JobID, failedAttemptedRunA.RunID),
				},
			},
			idsOfJobsToSchedule: []string{queuedJobA.JobID},
		},
		// TODO(albin): Also test a job being attempted to many times.
		"Running job is preempted": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					runningJobA,
				},
				runUpdates: []*database.Run{
					newRunA,
				},
			},
			idsOfJobsToPreempt:      []string{queuedJobA.JobID},
			expectedJobDbCycleThree: make([]*jobdb.Job, 0),
			expectedEventSequencesCycleThree: []*armadaevents.EventSequence{
				{
					Queue:      queuedJobA.Queue,
					JobSetName: queuedJobA.JobSet,
					Events: []*armadaevents.EventSequence_Event{
						{
							Created: &types.Timestamp{},
							Event: &armadaevents.EventSequence_Event_JobRunPreempted{
								JobRunPreempted: &armadaevents.JobRunPreempted{
									PreemptedRunId: testfixtures.UUIDFromInt(1).String(),
									PreemptedJobId: queuedJobA.JobID,
								},
							},
						},
						{
							Created: &types.Timestamp{},
							Event: &armadaevents.EventSequence_Event_JobRunErrors{
								JobRunErrors: &armadaevents.JobRunErrors{
									JobId: queuedJobA.JobID,
									RunId: testfixtures.UUIDFromInt(1).String(),
									Errors: []*armadaevents.Error{
										{
											Terminal:           true,
											FailureCategory:    errormatch.CategoryPreemption,
											FailureSubcategory: string(schedulercontext.Unknown),
											Reason: &armadaevents.Error_JobRunPreemptedError{
												JobRunPreemptedError: &armadaevents.JobRunPreemptedError{},
											},
										},
									},
								},
							},
						},
						{
							Created: &types.Timestamp{},
							Event: &armadaevents.EventSequence_Event_JobErrors{
								JobErrors: &armadaevents.JobErrors{
									JobId: queuedJobA.JobID,
									Errors: []*armadaevents.Error{
										{
											Terminal:           true,
											FailureCategory:    errormatch.CategoryPreemption,
											FailureSubcategory: string(schedulercontext.Unknown),
											Reason: &armadaevents.Error_JobRunPreemptedError{
												JobRunPreemptedError: &armadaevents.JobRunPreemptedError{},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		"Queued job is cancelled": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					queuedJobA,
				},
			},
			secondSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					cancelRequestedJobA,
				},
			},
		},
		"Running job is cancelled": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					runningJobA,
				},
				runUpdates: []*database.Run{
					newRunA,
				},
			},
			secondSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					runningCancelRequestedJobA,
				},
			},
		},
		"Queued job is cancelled by job set": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					queuedJobA,
				},
			},
			secondSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					cancelByJobSetRequestedJobA,
				},
			},
		},
		"Running job is cancelled by job set": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					runningJobA,
				},
				runUpdates: []*database.Run{
					newRunA,
				},
			},
			secondSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					runningCancelByJobSetRequestedJobA,
				},
			},
		},
		"Queued job is re-prioritised": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					queuedJobA,
				},
			},
			secondSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					queuedJobWithUpdatedPriorityA,
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := armadacontext.Background()
			testClock := clock.NewFakeClock(time.Unix(0, 0))

			// Setup necessary for creating schedulerDb operations.
			queueByJobId := make(map[string]string)
			jobSetByJobId := make(map[string]string)
			for _, jobUpdate := range tc.firstSchedulerDbUpdate.jobUpdates {
				queueByJobId[jobUpdate.JobID] = jobUpdate.Queue
				jobSetByJobId[jobUpdate.JobID] = jobUpdate.JobSet
			}
			for _, jobUpdate := range tc.secondSchedulerDbUpdate.jobUpdates {
				queueByJobId[jobUpdate.JobID] = jobUpdate.Queue
				jobSetByJobId[jobUpdate.JobID] = jobUpdate.JobSet
			}
			instructionConverter, err := scheduleringester.NewJobSetEventsInstructionConverter(nil)
			require.NoError(t, err)

			// Declared here so the cycle/persist closures can reach the current
			// algo, but (re)assigned per withTestSetup invocation below. The algo's
			// `persisted` latch must start clear for each scenario; otherwise the
			// scheduling decisions of one scenario leak into the next.
			var (
				testAlgo     *testSchedulingAlgo
				sharedRunner runner.SchedulingRunner
			)

			// Helper function for creating new schedulers for use in tests.
			newScheduler := func(db *pgxpool.Pool) *Scheduler {
				scheduler, err := NewScheduler(
					testfixtures.NewJobDb(resourceListFactory),
					database.NewPostgresJobRepository(db, 1024),
					&testExecutorRepository{
						updateTimes: map[string]time.Time{"test-executor": testClock.Now()},
					},
					sharedRunner,
					leaderelection.NewStandaloneLeaderController(),
					newTestPublisher(),
					&testSubmitChecker{
						checkSuccess: !tc.failSubmitCheck,
					},
					&testGangValidator{
						validateSuccess: true,
					},
					1*time.Second,
					5*time.Second,
					0,
					nil,
					maxNumberOfAttempts,
					nodeIdLabel,
					schedulerMetrics,
					pricing.NoopBidPriceProvider{},
					[]string{},
					&testQueueCache{},
					schedulerconfig.RetryPolicyConfig{},
					retry.NoopPolicyCache{},
				)
				require.NoError(t, err)
				scheduler.clock = testClock
				scheduler.EnableAssertions()
				return scheduler
			}

			withTestSetup := func(f func(a, b *Scheduler, schedulerDb *scheduleringester.SchedulerDb) error) error {
				return schedulerdb.WithTestDb(func(_ *schedulerdb.Queries, db *pgxpool.Pool) error {
					// Create a schedulerDb using this db connection and initialise its state.
					schedulerDb := scheduleringester.NewSchedulerDb(
						db,
						nil,
						time.Second,
						time.Second,
						10*time.Second,
					)

					// Fresh algo + runner per scenario. Both schedulers below share this
					// single instance so they make the same scheduling decisions, but a new
					// instance per scenario keeps the algo's `persisted` latch from leaking
					// decisions across scenarios.
					testAlgo = &testSchedulingAlgo{
						jobsToSchedule: tc.idsOfJobsToSchedule,
						jobsToPreempt:  tc.idsOfJobsToPreempt,
					}
					sharedRunner = runner.NewSyncSchedulingRunner(testAlgo)

					// Create two scheduler using the same db connection. Both share
					// `sharedRunner` so they make the same scheduling decisions.
					a := newScheduler(db)
					b := newScheduler(db)

					// Initially, "a" is leader and "b" follower.
					b.leaderController.(*leaderelection.StandaloneLeaderController).SetToken(leaderelection.InvalidLeaderToken())

					return f(a, b, schedulerDb)
				})
			}

			// Helper function for {first,second}DbUpdate.
			dbUpdate := func(
				schedulerDb *scheduleringester.SchedulerDb,
				jobUpdates []*database.Job,
				runUpdates []*database.Run,
				jobRunErrors []*armadaevents.JobRunErrors,
			) error {
				dbOps, err := dbOpsFromDbObjects(
					ctx,
					instructionConverter,
					queueByJobId,
					jobSetByJobId,
					jobUpdates,
					runUpdates,
					jobRunErrors,
				)
				if err != nil {
					return err
				}
				return schedulerDb.Store(ctx, &scheduleringester.DbOperationsWithMessageIds{Ops: dbOps})
			}

			// write the first schedulerDb update to postgres.
			firstDbUpdate := func(schedulerDb *scheduleringester.SchedulerDb) error {
				t.Log("writing first schedulerDb update")
				return dbUpdate(
					schedulerDb,
					tc.firstSchedulerDbUpdate.jobUpdates,
					tc.firstSchedulerDbUpdate.runUpdates,
					tc.firstSchedulerDbUpdate.jobRunErrors,
				)
			}

			// write the second schedulerDb update to postgres.
			secondDbUpdate := func(schedulerDb *scheduleringester.SchedulerDb) error {
				t.Log("writing second schedulerDb update")
				return dbUpdate(
					schedulerDb,
					tc.secondSchedulerDbUpdate.jobUpdates,
					tc.secondSchedulerDbUpdate.runUpdates,
					tc.secondSchedulerDbUpdate.jobRunErrors,
				)
			}

			// cycle runs one cycle.
			cycle := func(s *Scheduler, updateAll, shouldSchedule bool) error {
				t.Logf("cycle scheduler %p", s)
				_, err := s.cycle(ctx, updateAll, s.leaderController.GetToken(), shouldSchedule, 1)
				return err
			}

			// persist persists to the schedulerDb any published eventSequences.
			persist := func(s *Scheduler, schedulerDb *scheduleringester.SchedulerDb) error {
				publisher := s.publisher.(*testPublisher)
				events := publisher.ReadAll()
				sequences := &utils.EventsWithIds[*armadaevents.EventSequence]{
					Events: events,
				}
				dbOpsWithMessageIds := instructionConverter.Convert(ctx, sequences)

				// Mark scheduling decisions as persisted.
				// If not persisted, the same jobs are scheduled again on subsequent calls to schedule.
				testAlgo.Persist()

				// Logging
				numEvents := 0
				for _, eventSequence := range sequences.Events {
					numEvents += len(eventSequence.Events)
				}
				t.Logf("persist scheduler %p; %d eventSequences, %d eventSequences, %d ops", s, len(sequences.Events), numEvents, len(dbOpsWithMessageIds.Ops))
				for _, eventSequence := range sequences.Events {
					for _, event := range eventSequence.Events {
						t.Logf("\tevent %s, %s, %T", eventSequence.Queue, eventSequence.JobSetName, event.Event)
					}
				}
				for _, op := range dbOpsWithMessageIds.Ops {
					t.Logf("\toperation %T: %v", op, op)
				}

				return schedulerDb.Store(ctx, dbOpsWithMessageIds)
			}

			// failover swaps the leader tokens between a and b, thus swapping which scheduler is leader.
			failover := func(a, b *Scheduler) error {
				t.Logf("failover schedulers %p, %p", a, b)
				lca := a.leaderController.(*leaderelection.StandaloneLeaderController)
				lcb := b.leaderController.(*leaderelection.StandaloneLeaderController)
				lta := lca.GetToken()
				ltb := lcb.GetToken()
				lca.SetToken(ltb)
				lcb.SetToken(lta)
				return nil
			}

			eventsFromTestPublisher := func(p publisher.Publisher) []*armadaevents.EventSequence {
				return p.(*testPublisher).eventSequences
			}

			var (
				jobDbCycleOne   *jobdb.JobDb
				jobDbCycleTwo   *jobdb.JobDb
				jobDbCycleThree *jobdb.JobDb

				eventsCycleOne   []*armadaevents.EventSequence
				eventsCycleTwo   []*armadaevents.EventSequence
				eventsCycleThree []*armadaevents.EventSequence
			)

			// One scheduler performing three cycles.
			// Used for absolute assertions and as a baseline for relative assertions.
			t.Log("scenario 1")
			require.NoError(t, withTestSetup(func(a, _ *Scheduler, schedulerDb *scheduleringester.SchedulerDb) error {
				require.NoError(t, firstDbUpdate(schedulerDb))
				require.NoError(t, cycle(a, false, true))
				require.NoError(t, persist(a, schedulerDb))
				jobDbCycleOne = a.jobDb.Clone()
				eventsCycleOne = slices.Clone(eventsFromTestPublisher(a.publisher))

				require.NoError(t, secondDbUpdate(schedulerDb))
				require.NoError(t, cycle(a, false, true))
				require.NoError(t, persist(a, schedulerDb))
				jobDbCycleTwo = a.jobDb.Clone()
				eventsCycleTwo = slices.Clone(eventsFromTestPublisher(a.publisher))

				require.NoError(t, cycle(a, false, true))
				require.NoError(t, persist(a, schedulerDb))
				jobDbCycleThree = a.jobDb.Clone()
				eventsCycleThree = slices.Clone(eventsFromTestPublisher(a.publisher))

				return nil
			}))

			// Absolute assertions.
			if tc.expectedJobDbCycleOne != nil {
				jobDb := testfixtures.NewJobDbWithJobs(tc.expectedJobDbCycleOne)
				require.NoError(t, jobDb.ReadTxn().AssertEqual(jobDbCycleOne.ReadTxn()), "unexpected cycle one jobDb")
			}
			if tc.expectedJobDbCycleTwo != nil {
				jobDb := testfixtures.NewJobDbWithJobs(tc.expectedJobDbCycleTwo)
				require.NoError(t, jobDb.ReadTxn().AssertEqual(jobDbCycleTwo.ReadTxn()), "unexpected cycle two jobDb")
			}
			if tc.expectedJobDbCycleThree != nil {
				jobDb := testfixtures.NewJobDbWithJobs(tc.expectedJobDbCycleThree)
				require.NoError(t, jobDb.ReadTxn().AssertEqual(jobDbCycleThree.ReadTxn()), "unexpected cycle three jobDb")
			}

			if tc.expectedEventSequencesCycleOne != nil {
				requireEventSequencesEqual(t, tc.expectedEventSequencesCycleOne, eventsCycleOne, "unexpected cycle one events")
			}
			if tc.expectedEventSequencesCycleTwo != nil {
				requireEventSequencesEqual(t, tc.expectedEventSequencesCycleTwo, eventsCycleTwo, "unexpected cycle two events")
			}
			if tc.expectedEventSequencesCycleThree != nil {
				requireEventSequencesEqual(t, tc.expectedEventSequencesCycleThree, eventsCycleThree, "unexpected cycle three events")
			}

			// Test that the follower stays in sync with the leader.
			// TODO(albin): We need to cycle "a" again after each persist for now.
			//              The jobDb of the leader is supposed to be updated immediately to reflect published events.
			//              However, the jobDb is only updated immediately for schedule/preempt/fail decisions.
			//              For external changes, e.g., job succeeded, the jobDb is only updated on the second cycle.
			// TODO(albin): We actually need to fix this since we may preempt an already successful job as it is.
			// TODO(albin): Can we become leader again after failing over to become a follower when leader?
			//              Let's test multiple failovers where you go back and forth.
			t.Log("scenario 2")
			// TODO(albin): Compare a against baseline a.
			require.NoError(t, withTestSetup(func(a, b *Scheduler, schedulerDb *scheduleringester.SchedulerDb) error {
				require.NoError(t, firstDbUpdate(schedulerDb))
				require.NoError(t, cycle(a, false, true))
				require.NoError(t, jobDbCycleOne.ReadTxn().AssertEqual(a.jobDb.ReadTxn()))
				require.NoError(t, persist(a, schedulerDb))
				require.NoError(t, cycle(a, false, true))
				require.NoError(t, cycle(b, false, false))
				require.NoError(t, a.jobDb.ReadTxn().AssertEqual(b.jobDb.ReadTxn()))
				require.Empty(t, eventsFromTestPublisher(b.publisher))

				require.NoError(t, secondDbUpdate(schedulerDb))
				require.NoError(t, cycle(a, false, true))
				require.NoError(t, persist(a, schedulerDb))
				require.NoError(t, jobDbCycleTwo.ReadTxn().AssertEqual(a.jobDb.ReadTxn()))
				require.NoError(t, cycle(a, false, true))
				require.NoError(t, cycle(b, false, false))
				require.NoError(t, a.jobDb.ReadTxn().AssertEqual(b.jobDb.ReadTxn()))

				require.NoError(t, cycle(a, false, true))
				require.NoError(t, persist(a, schedulerDb))
				require.NoError(t, cycle(b, false, false))
				require.NoError(t, a.jobDb.ReadTxn().AssertEqual(b.jobDb.ReadTxn()))

				return nil
			}))

			// Test that the follower stays in sync with the leader when the follower cycles once before "a" persists.
			t.Log("scenario 3")
			require.NoError(t, withTestSetup(func(a, b *Scheduler, schedulerDb *scheduleringester.SchedulerDb) error {
				require.NoError(t, firstDbUpdate(schedulerDb))
				require.NoError(t, cycle(a, false, true))
				require.NoError(t, cycle(b, false, false))
				require.NoError(t, persist(a, schedulerDb))
				require.NoError(t, cycle(a, false, true))
				require.NoError(t, cycle(b, false, false))
				require.NoError(t, a.jobDb.ReadTxn().AssertEqual(b.jobDb.ReadTxn()))

				require.NoError(t, secondDbUpdate(schedulerDb))
				require.NoError(t, cycle(a, false, true))
				require.NoError(t, cycle(b, false, false))
				require.NoError(t, persist(a, schedulerDb))
				require.NoError(t, cycle(a, false, true))
				require.NoError(t, cycle(b, false, false))
				require.NoError(t, a.jobDb.ReadTxn().AssertEqual(b.jobDb.ReadTxn()))

				require.NoError(t, cycle(a, false, true))
				require.NoError(t, cycle(b, false, false))
				require.NoError(t, persist(a, schedulerDb))
				require.NoError(t, cycle(b, false, false))
				require.NoError(t, a.jobDb.ReadTxn().AssertEqual(b.jobDb.ReadTxn()))

				return nil
			}))

			// Test follower failover.
			t.Log("scenario 4")
			require.NoError(t, withTestSetup(func(a, b *Scheduler, schedulerDb *scheduleringester.SchedulerDb) error {
				require.NoError(t, firstDbUpdate(schedulerDb))
				require.NoError(t, cycle(b, false, false))

				require.Empty(t, eventsFromTestPublisher(b.publisher))
				require.NoError(t, failover(a, b))
				require.NoError(t, cycle(b, true, true))
				require.NoError(t, persist(b, schedulerDb))
				require.NoError(t, jobDbCycleOne.ReadTxn().AssertEqual(b.jobDb.ReadTxn()))

				require.NoError(t, secondDbUpdate(schedulerDb))
				require.NoError(t, cycle(b, false, true))
				require.NoError(t, persist(b, schedulerDb))
				require.NoError(t, jobDbCycleTwo.ReadTxn().AssertEqual(b.jobDb.ReadTxn()))

				require.NoError(t, cycle(b, false, true))
				require.NoError(t, persist(b, schedulerDb))
				require.NoError(t, jobDbCycleThree.ReadTxn().AssertEqual(b.jobDb.ReadTxn()))

				return nil
			}))

			// Test follower failover after the first db update.
			t.Log("scenario 5")
			require.NoError(t, withTestSetup(func(a, b *Scheduler, schedulerDb *scheduleringester.SchedulerDb) error {
				require.NoError(t, firstDbUpdate(schedulerDb))
				require.NoError(t, cycle(a, false, true))
				require.NoError(t, persist(a, schedulerDb))

				require.NoError(t, secondDbUpdate(schedulerDb))
				require.NoError(t, failover(a, b))
				require.NoError(t, cycle(b, true, true))
				require.NoError(t, persist(b, schedulerDb))
				require.NoError(t, jobDbCycleTwo.ReadTxn().AssertEqual(b.jobDb.ReadTxn()))

				require.NoError(t, cycle(b, false, true))
				require.NoError(t, persist(b, schedulerDb))
				require.NoError(t, jobDbCycleThree.ReadTxn().AssertEqual(b.jobDb.ReadTxn()))

				return nil
			}))

			// Test follower failover after the first db update, when the follower has already cycled once.
			t.Log("scenario 6")
			require.NoError(t, withTestSetup(func(a, b *Scheduler, schedulerDb *scheduleringester.SchedulerDb) error {
				require.NoError(t, firstDbUpdate(schedulerDb))
				require.NoError(t, cycle(a, false, true))
				require.NoError(t, persist(a, schedulerDb))

				require.NoError(t, secondDbUpdate(schedulerDb))
				require.NoError(t, cycle(b, false, false))
				require.NoError(t, failover(a, b))
				require.NoError(t, cycle(b, true, true))
				require.NoError(t, persist(b, schedulerDb))
				require.NoError(t, jobDbCycleTwo.ReadTxn().AssertEqual(b.jobDb.ReadTxn()))

				require.NoError(t, cycle(b, false, true))
				require.NoError(t, persist(b, schedulerDb))
				require.NoError(t, jobDbCycleThree.ReadTxn().AssertEqual(b.jobDb.ReadTxn()))

				return nil
			}))

			// Test follower failover after the first db update, when the follower has already cycled once before the second update.
			t.Log("scenario 7")
			require.NoError(t, withTestSetup(func(a, b *Scheduler, schedulerDb *scheduleringester.SchedulerDb) error {
				require.NoError(t, firstDbUpdate(schedulerDb))
				require.NoError(t, cycle(a, false, true))
				require.NoError(t, persist(a, schedulerDb))

				require.NoError(t, cycle(b, false, false))
				require.NoError(t, secondDbUpdate(schedulerDb))
				require.NoError(t, failover(a, b))
				require.NoError(t, cycle(b, true, true))
				require.NoError(t, persist(b, schedulerDb))
				require.NoError(t, jobDbCycleTwo.ReadTxn().AssertEqual(b.jobDb.ReadTxn()))

				require.NoError(t, cycle(b, false, true))
				require.NoError(t, persist(b, schedulerDb))
				require.NoError(t, jobDbCycleThree.ReadTxn().AssertEqual(b.jobDb.ReadTxn()))

				return nil
			}))
		})
	}
}

func dbOpsFromDbObjects(
	ctx *armadacontext.Context,
	instructionConverter *scheduleringester.JobSetEventsInstructionConverter,
	queueByJobId map[string]string,
	jobSetByJobId map[string]string,
	jobUpdates []*database.Job,
	runUpdates []*database.Run,
	jobRunErrors []*armadaevents.JobRunErrors,
) ([]scheduleringester.DbOperation, error) {
	dbOps := make([]scheduleringester.DbOperation, 0)

	// jobUpdatesByJobId := make(map[string]*database.Job)
	insertJobsDbOp := make(scheduleringester.InsertJobs, len(jobUpdates))
	for _, dbJob := range jobUpdates {
		insertJobsDbOp[dbJob.JobID] = &scheduleringester.JobInsertion{Job: dbJob}
		// jobUpdatesByJobId[dbJob.JobID] = dbJob
	}
	dbOps = scheduleringester.AppendDbOperation(dbOps, fixInsertJobsDbOp(insertJobsDbOp))

	insertRunsDbOp := make(scheduleringester.InsertRuns, len(runUpdates))
	for _, dbRun := range runUpdates {
		queue, ok := queueByJobId[dbRun.JobID]
		if !ok {
			return nil, errors.Errorf("run %s is associated with non-existing job %s", dbRun.RunID, dbRun.JobID)
		}
		insertRunsDbOp[dbRun.RunID] = &scheduleringester.JobRunDetails{
			Queue: queue,
			DbRun: dbRun,
		}
	}
	dbOps = scheduleringester.AppendDbOperation(dbOps, insertRunsDbOp)

	jobRunErrorsEventSequences := make([]*armadaevents.EventSequence, 0, len(jobRunErrors))
	for _, jobRunError := range jobRunErrors {
		jobId := jobRunError.JobId
		queue, ok := queueByJobId[jobId]
		if !ok {
			return nil, errors.Errorf("jobRunError is associated with non-existing job %s", jobId)
		}
		jobSet, ok := jobSetByJobId[jobId]
		if !ok {
			return nil, errors.Errorf("jobRunError is associated with non-existing job %s", jobId)
		}
		eventSequence := &armadaevents.EventSequence{
			Queue:      queue,
			JobSetName: jobSet,
			Events: []*armadaevents.EventSequence_Event{
				{
					Event: &armadaevents.EventSequence_Event_JobRunErrors{JobRunErrors: jobRunError},
				},
			},
		}
		jobRunErrorsEventSequences = append(jobRunErrorsEventSequences, eventSequence)
	}
	insertJobRunErrorsDbOps := instructionConverter.Convert(
		ctx,
		&utils.EventsWithIds[*armadaevents.EventSequence]{
			Events: jobRunErrorsEventSequences,
		},
	)
	for _, dbOp := range insertJobRunErrorsDbOps.Ops {
		dbOps = scheduleringester.AppendDbOperation(dbOps, dbOp)
	}

	return dbOps, nil
}

func fixInsertJobsDbOp(dbOp scheduleringester.InsertJobs) scheduleringester.InsertJobs {
	for _, job := range dbOp {
		// This field must be non-null when written to postgres.
		job.Metadata.SubmitMessage = make([]byte, 0)
	}
	return dbOp
}
