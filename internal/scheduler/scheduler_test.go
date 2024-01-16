package scheduler

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/ingest"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/util"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/database"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/kubernetesobjects/affinity"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/internal/scheduleringester"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Data to be used in tests
const (
	maxNumberOfAttempts = 2
	nodeIdLabel         = "kubernetes.io/hostname"
)

var (
	failFastSchedulingInfo = &schedulerobjects.JobSchedulingInfo{
		AtMostOnce: true,
		ObjectRequirements: []*schedulerobjects.ObjectRequirements{
			{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: &schedulerobjects.PodRequirements{
						Annotations: map[string]string{
							configuration.FailFastAnnotation: "true",
						},
					},
				},
			},
		},
		Version: 1,
	}
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
	schedulingInfoWithQueueTtl = &schedulerobjects.JobSchedulingInfo{
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
		QueueTtlSeconds: 2,
		Version:         1,
	}
	schedulingInfoWithQueueTtlBytes = protoutil.MustMarshall(schedulingInfoWithQueueTtl)
	schedulerMetrics                = NewSchedulerMetrics(configuration.SchedulerMetricsConfig{
		ScheduleCycleTimeHistogramSettings: configuration.HistogramConfig{
			Start:  1,
			Factor: 1.1,
			Count:  100,
		},
		ReconcileCycleTimeHistogramSettings: configuration.HistogramConfig{
			Start:  1,
			Factor: 1.1,
			Count:  100,
		},
	})
)

var queuedJob = testfixtures.JobDb.NewJob(
	util.NewULID(),
	"testJobset",
	"testQueue",
	uint32(10),
	schedulingInfo,
	true,
	0,
	false,
	false,
	false,
	1,
)

var queuedJobTwo = testfixtures.JobDb.NewJob(
	util.NewULID(),
	"testJobset",
	"testQueue",
	uint32(10),
	schedulingInfo,
	true,
	0,
	false,
	false,
	false,
	1,
)

var queuedJobWithExpiredTtl = testfixtures.JobDb.NewJob(
	util.NewULID(),
	"testJobset",
	"testQueue",
	0,
	schedulingInfoWithQueueTtl,
	true,
	0,
	false,
	false,
	false,
	1,
)

var leasedJob = testfixtures.JobDb.NewJob(
	util.NewULID(),
	"testJobset",
	"testQueue",
	uint32(10),
	schedulingInfo,
	false,
	1,
	false,
	false,
	false,
	1,
).WithNewRun("testExecutor", "test-node", "node", 5)

var returnedOnceLeasedJob = testfixtures.JobDb.NewJob(
	"01h3w2wtdchtc80hgyp782shrv",
	"testJobset",
	"testQueue",
	uint32(10),
	schedulingInfo,
	false,
	1,
	false,
	false,
	false,
	1,
).WithUpdatedRun(testfixtures.JobDb.CreateRun(
	uuid.New(),
	"01h3w2wtdchtc80hgyp782shrv",
	0,
	"testExecutor",
	"testNodeId",
	"testNodeName",
	&scheduledAtPriority,
	false,
	false,
	true,
	false,
	true,
	true,
)).WithNewRun("testExecutor", "test-node", "node", 5)

var defaultJobError = &armadaevents.Error{
	Terminal: true,
	Reason: &armadaevents.Error_PodError{
		PodError: &armadaevents.PodError{
			Message: "generic pod error",
		},
	},
}

func defaultJobRunError(jobId string, runId uuid.UUID) *armadaevents.JobRunErrors {
	protoJobId, err := armadaevents.ProtoUuidFromUlidString(jobId)
	if err != nil {
		panic(err)
	}
	protoRunId := armadaevents.ProtoUuidFromUuid(runId)
	return &armadaevents.JobRunErrors{
		RunId: protoRunId,
		JobId: protoJobId,
		Errors: []*armadaevents.Error{
			defaultJobError,
		},
	}
}

var leasedFailFastJob = testfixtures.JobDb.NewJob(
	util.NewULID(),
	"testJobset",
	"testQueue",
	uint32(10),
	failFastSchedulingInfo,
	false,
	0,
	false,
	false,
	false,
	1,
).WithNewRun("testExecutor", "test-node", "node", 5)

var (
	testExecutor        = "test-executor"
	testNode            = "test-node"
	testNodeId          = api.NodeIdFromExecutorAndNodeName(testExecutor, testNode)
	scheduledAtPriority = int32(10)
	requeuedJobId       = util.NewULID()
	requeuedJob         = testfixtures.JobDb.NewJob(
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
		1,
	).WithUpdatedRun(testfixtures.JobDb.CreateRun(
		uuid.New(),
		requeuedJobId,
		time.Now().Unix(),
		"testExecutor",
		"test-node",
		"node",
		&scheduledAtPriority,
		false,
		false,
		true,
		false,
		true,
		true,
	))
)

// Test a single scheduler cycle
func TestScheduler_TestCycle(t *testing.T) {
	tests := map[string]struct {
		initialJobs                      []*jobdb.Job                      // jobs in the jobDb at the start of the cycle
		jobUpdates                       []database.Job                    // job updates from the database
		runUpdates                       []database.Run                    // run updates from the database
		jobRunErrors                     map[uuid.UUID]*armadaevents.Error // job run errors in the database
		staleExecutor                    bool                              // if true then the executorRepository will report the executor as stale
		fetchError                       bool                              // if true then the jobRepository will throw an error
		scheduleError                    bool                              // if true then the scheduling algo will throw an error
		publishError                     bool                              // if true the publisher will throw an error
		submitCheckerFailure             bool                              // if true the submit checker will say the job is unschedulable
		expectedJobRunLeased             []string                          // ids of jobs we expect to have produced leased messages
		expectedJobRunErrors             []string                          // ids of jobs we expect to have produced jobRunErrors messages
		expectedJobErrors                []string                          // ids of jobs we expect to have produced jobErrors messages
		expectedJobsToFail               []string                          // ids of jobs we expect to fail without having failed the overall scheduling cycle
		expectedJobRunPreempted          []string                          // ids of jobs we expect to have produced jobRunPreempted messages
		expectedJobCancelled             []string                          // ids of jobs we expect to have  produced cancelled messages
		expectedJobRequestCancel         []string                          // ids of jobs we expect to have produced request cancel
		expectedJobReprioritised         []string                          // ids of jobs we expect to have  produced reprioritised messages
		expectedQueued                   []string                          // ids of jobs we expect to have  produced requeued messages
		expectedJobSucceeded             []string                          // ids of jobs we expect to have  produced succeeeded messages
		expectedLeased                   []string                          // ids of jobs we expected to be leased in jobdb at the end of the cycle
		expectedRequeued                 []string                          // ids of jobs we expected to be requeued in jobdb at the end of the cycle
		expectedTerminal                 []string                          // ids of jobs we expected to be terminal in jobdb at the end of the cycle
		expectedJobPriority              map[string]uint32                 // expected priority of jobs at the end of the cycle
		expectedNodeAntiAffinities       []string                          // list of nodes there is expected to be anti affinities for on job scheduling info
		expectedJobSchedulingInfoVersion int                               // expected scheduling info version of jobs at the end of the cycle
		expectedQueuedVersion            int32                             // expected queued version of jobs at the end of the cycle
	}{
		"Lease a single job already in the db": {
			initialJobs:           []*jobdb.Job{queuedJob},
			expectedJobRunLeased:  []string{queuedJob.Id()},
			expectedLeased:        []string{queuedJob.Id()},
			expectedQueuedVersion: queuedJob.QueuedVersion(),
		},
		"Lease a single job from an update": {
			jobUpdates: []database.Job{
				{
					JobID:                 queuedJob.Id(),
					JobSet:                "testJobSet",
					Queue:                 "testQueue",
					Queued:                true,
					QueuedVersion:         0,
					SchedulingInfo:        schedulingInfoBytes,
					SchedulingInfoVersion: int32(schedulingInfo.Version),
					Serial:                1,
				},
			},
			expectedJobRunLeased:  []string{queuedJob.Id()},
			expectedLeased:        []string{queuedJob.Id()},
			expectedQueuedVersion: 0,
		},
		"Lease two jobs from an update": {
			jobUpdates: []database.Job{
				{
					JobID:                 "01h3w2wtdchtc80hgyp782shrv",
					JobSet:                "testJobSet",
					Queue:                 "testQueue",
					Queued:                true,
					QueuedVersion:         0,
					SchedulingInfo:        schedulingInfoBytes,
					SchedulingInfoVersion: int32(schedulingInfo.Version),
					Serial:                1,
				},
				{
					JobID:                 "01h434g4hxww2pknb2q1nfmfph",
					JobSet:                "testJobSet",
					Queue:                 "testQueue",
					Queued:                true,
					QueuedVersion:         0,
					SchedulingInfo:        schedulingInfoBytes,
					SchedulingInfoVersion: int32(schedulingInfo.Version),
					Serial:                1,
				},
			},
			expectedJobRunLeased:  []string{"01h3w2wtdchtc80hgyp782shrv", "01h434g4hxww2pknb2q1nfmfph"},
			expectedLeased:        []string{"01h3w2wtdchtc80hgyp782shrv", "01h434g4hxww2pknb2q1nfmfph"},
			expectedQueuedVersion: 0,
		},
		"New failed job with no runs": {
			// This happens if the scheduler decides to fail a job, e.g., due to min-max gang scheduling.
			jobUpdates: []database.Job{
				{
					JobID:                 queuedJob.Id(),
					JobSet:                "testJobSet",
					Queue:                 "testQueue",
					Failed:                true,
					QueuedVersion:         0,
					SchedulingInfo:        schedulingInfoBytes,
					SchedulingInfoVersion: int32(schedulingInfo.Version),
					Serial:                1,
				},
			},
			expectedQueuedVersion: 0,
		},
		"Queued job transitions straight to failed without running": {
			// This happens if the scheduler decides to fail a job, e.g., due to min-max gang scheduling.
			initialJobs: []*jobdb.Job{queuedJob},
			jobUpdates: []database.Job{
				{
					JobID:                 queuedJob.Id(),
					JobSet:                "testJobSet",
					Queue:                 "testQueue",
					Failed:                true,
					QueuedVersion:         0,
					SchedulingInfo:        schedulingInfoBytes,
					SchedulingInfoVersion: int32(schedulingInfo.Version),
					Serial:                1,
				},
			},
			expectedQueuedVersion: 0,
		},
		"Nothing leased": {
			initialJobs:           []*jobdb.Job{queuedJob},
			expectedQueued:        []string{queuedJob.Id()},
			expectedQueuedVersion: queuedJob.QueuedVersion(),
		},
		"FailedJobs in scheduler result will publish appropriate messages": {
			initialJobs:        []*jobdb.Job{queuedJob},
			expectedJobErrors:  []string{queuedJob.Id()},
			expectedJobsToFail: []string{queuedJob.Id()},
			expectedTerminal:   []string{queuedJob.Id()},
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
					QueuedVersion:         1,
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
					Node:         "node",
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
			initialJobs: []*jobdb.Job{returnedOnceLeasedJob},
			runUpdates: []database.Run{
				{
					RunID:        returnedOnceLeasedJob.LatestRun().Id(),
					JobID:        returnedOnceLeasedJob.Id(),
					JobSet:       "testJobSet",
					Executor:     "testExecutor",
					Node:         "testNode",
					Failed:       true,
					Returned:     true,
					RunAttempted: true,
					Serial:       2,
				},
			},
			expectedJobErrors:     []string{returnedOnceLeasedJob.Id()},
			expectedTerminal:      []string{returnedOnceLeasedJob.Id()},
			expectedQueuedVersion: 1,
		},
		"Lease returned for fail fast job": {
			initialJobs: []*jobdb.Job{leasedFailFastJob},
			// Fail fast should mean there is only ever 1 attempted run
			runUpdates: []database.Run{
				{
					RunID:        leasedFailFastJob.LatestRun().Id(),
					JobID:        leasedFailFastJob.Id(),
					JobSet:       "testJobSet",
					Executor:     "testExecutor",
					Failed:       true,
					Returned:     true,
					RunAttempted: false,
					Serial:       1,
				},
			},
			expectedJobErrors:     []string{leasedFailFastJob.Id()},
			expectedTerminal:      []string{leasedFailFastJob.Id()},
			expectedQueuedVersion: leasedFailFastJob.QueuedVersion(),
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
		"New job from postgres with expired queue ttl is cancel requested": {
			jobUpdates: []database.Job{
				{
					JobID:          queuedJobWithExpiredTtl.Id(),
					JobSet:         queuedJobWithExpiredTtl.Jobset(),
					Queue:          queuedJobWithExpiredTtl.Queue(),
					Queued:         queuedJobWithExpiredTtl.Queued(),
					QueuedVersion:  queuedJobWithExpiredTtl.QueuedVersion(),
					Serial:         1,
					Submitted:      queuedJobWithExpiredTtl.Created(),
					SchedulingInfo: schedulingInfoWithQueueTtlBytes,
				},
			},

			// We expect to publish request cancel and cancelled message this cycle.
			// The job should also be removed from the queue and set to a terminal state.
			expectedJobRequestCancel: []string{queuedJobWithExpiredTtl.Id()},
			expectedJobCancelled:     []string{queuedJobWithExpiredTtl.Id()},
			expectedQueuedVersion:    queuedJobWithExpiredTtl.QueuedVersion(),
			expectedTerminal:         []string{queuedJobWithExpiredTtl.Id()},
		},
		"Existing jobDb job with expired queue ttl is cancel requested": {
			initialJobs: []*jobdb.Job{queuedJobWithExpiredTtl},

			// We expect to publish request cancel and cancelled message this cycle.
			// The job should also be removed from the queue and set to a terminal state.
			expectedJobRequestCancel: []string{queuedJobWithExpiredTtl.Id()},
			expectedJobCancelled:     []string{queuedJobWithExpiredTtl.Id()},
			expectedQueuedVersion:    queuedJobWithExpiredTtl.QueuedVersion(),
			expectedTerminal:         []string{queuedJobWithExpiredTtl.Id()},
		},
		"New postgres job with cancel requested results in cancel messages": {
			jobUpdates: []database.Job{
				{
					JobID:           queuedJobWithExpiredTtl.Id(),
					JobSet:          queuedJobWithExpiredTtl.Jobset(),
					Queue:           queuedJobWithExpiredTtl.Queue(),
					Queued:          queuedJobWithExpiredTtl.Queued(),
					QueuedVersion:   queuedJobWithExpiredTtl.QueuedVersion(),
					Serial:          1,
					Submitted:       queuedJobWithExpiredTtl.Created(),
					CancelRequested: true,
					Cancelled:       false,
					SchedulingInfo:  schedulingInfoWithQueueTtlBytes,
				},
			},

			// We have already got a request cancel from the DB, so only publish a cancelled message.
			// The job should also be removed from the queue and set to a terminal state.#
			expectedJobCancelled:  []string{queuedJobWithExpiredTtl.Id()},
			expectedQueuedVersion: queuedJobWithExpiredTtl.QueuedVersion(),
			expectedTerminal:      []string{queuedJobWithExpiredTtl.Id()},
		},
		"Postgres job with cancel requested results in cancel messages": {
			initialJobs: []*jobdb.Job{queuedJobWithExpiredTtl.WithCancelRequested(true)},
			jobUpdates: []database.Job{
				{
					JobID:           queuedJobWithExpiredTtl.Id(),
					JobSet:          queuedJobWithExpiredTtl.Jobset(),
					Queue:           queuedJobWithExpiredTtl.Queue(),
					Queued:          queuedJobWithExpiredTtl.Queued(),
					QueuedVersion:   queuedJobWithExpiredTtl.QueuedVersion(),
					Serial:          1,
					Submitted:       queuedJobWithExpiredTtl.Created(),
					CancelRequested: true,
					Cancelled:       false,
					SchedulingInfo:  schedulingInfoWithQueueTtlBytes,
				},
			},

			// We have already got a request cancel from the DB/existing job state, so only publish a cancelled message.
			// The job should also be removed from the queue and set to a terminal state.
			expectedJobCancelled:  []string{queuedJobWithExpiredTtl.Id()},
			expectedQueuedVersion: queuedJobWithExpiredTtl.QueuedVersion(),
			expectedTerminal:      []string{queuedJobWithExpiredTtl.Id()},
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
			jobRunErrors: map[uuid.UUID]*armadaevents.Error{
				leasedJob.LatestRun().Id(): defaultJobError,
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
				errors:      tc.jobRunErrors,
				shouldError: tc.fetchError,
			}
			testClock := clock.NewFakeClock(time.Now())
			schedulingAlgo := &testSchedulingAlgo{
				jobsToSchedule: tc.expectedJobRunLeased,
				jobsToPreempt:  tc.expectedJobRunPreempted,
				jobsToFail:     tc.expectedJobsToFail,
				shouldError:    tc.scheduleError,
			}
			publisher := &testPublisher{shouldError: tc.publishError}
			submitChecker := &testSubmitChecker{checkSuccess: !tc.submitCheckerFailure}

			heartbeatTime := testClock.Now()
			if tc.staleExecutor {
				heartbeatTime = heartbeatTime.Add(-2 * clusterTimeout)
			}
			clusterRepo := &testExecutorRepository{
				updateTimes: map[string]time.Time{"testExecutor": heartbeatTime},
			}
			sched, err := NewScheduler(
				testfixtures.NewJobDb(),
				jobRepo,
				clusterRepo,
				schedulingAlgo,
				NewStandaloneLeaderController(),
				publisher,
				submitChecker,
				1*time.Second,
				5*time.Second,
				clusterTimeout,
				maxNumberOfAttempts,
				nodeIdLabel,
				schedulerMetrics,
				nil,
			)
			require.NoError(t, err)
			sched.EnableAssertions()

			sched.clock = testClock

			// insert initial jobs
			txn := sched.jobDb.WriteTxn()
			err = txn.Upsert(tc.initialJobs)
			require.NoError(t, err)
			txn.Commit()

			// run a scheduler cycle
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			_, err = sched.cycle(ctx, false, sched.leaderController.GetToken(), true)
			if tc.fetchError || tc.publishError || tc.scheduleError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// Assert that all expected eventSequences are generated and that all eventSequences are expected.
			outstandingEventsByType := map[string]map[string]bool{
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_JobRunLeased{}):     stringSet(tc.expectedJobRunLeased),
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_JobErrors{}):        stringSet(tc.expectedJobErrors),
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_JobRunErrors{}):     stringSet(tc.expectedJobRunErrors),
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_JobRunPreempted{}):  stringSet(tc.expectedJobRunPreempted),
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_CancelledJob{}):     stringSet(tc.expectedJobCancelled),
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_ReprioritisedJob{}): stringSet(tc.expectedJobReprioritised),
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_JobSucceeded{}):     stringSet(tc.expectedJobSucceeded),
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_JobRequeued{}):      stringSet(tc.expectedRequeued),
				fmt.Sprintf("%T", &armadaevents.EventSequence_Event_CancelJob{}):        stringSet(tc.expectedJobRequestCancel),
			}
			err = subtractEventsFromOutstandingEventsByType(publisher.eventSequences, outstandingEventsByType)
			require.NoError(t, err)
			for eventType, m := range outstandingEventsByType {
				assert.Empty(t, m, "%d outstanding eventSequences of type %s", len(m), eventType)
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
			jobs := sched.jobDb.ReadTxn().GetAll()
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
					assert.Equal(t, expectedPriority, job.Priority())
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

				// expectedQueuedVersion := int32(1)
				// if tc.expectedQueuedVersion != 0 {
				// 	expectedQueuedVersion = tc.expectedQueuedVersion
				// }
				assert.Equal(t, tc.expectedQueuedVersion, job.QueuedVersion())
				expectedSchedulingInfoVersion := 1
				if tc.expectedJobSchedulingInfoVersion != 0 {
					expectedSchedulingInfoVersion = tc.expectedJobSchedulingInfoVersion
				}
				assert.Equal(t, uint32(expectedSchedulingInfoVersion), job.JobSchedulingInfo().Version)
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
	sched, err := NewScheduler(
		testfixtures.NewJobDb(),
		&jobRepo,
		clusterRepo,
		schedulingAlgo,
		leaderController,
		publisher,
		submitChecker,
		1*time.Second,
		15*time.Second,
		1*time.Hour,
		maxNumberOfAttempts,
		nodeIdLabel,
		schedulerMetrics,
		nil,
	)
	require.NoError(t, err)
	sched.EnableAssertions()

	sched.clock = testClock

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())

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
	assert.Equal(t, 1, len(publisher.eventSequences))
	assert.Equal(t, schedulingAlgo.numberOfScheduleCalls, 1)

	// invalidate our leadership: we should not publish
	leaderController.token = InvalidLeaderToken()
	fireCycle()
	assert.Equal(t, 0, len(publisher.eventSequences))
	assert.Equal(t, schedulingAlgo.numberOfScheduleCalls, 1)

	// become master again: we should publish
	leaderController.token = NewLeaderToken()
	fireCycle()
	assert.Equal(t, 1, len(publisher.eventSequences))
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
					QueuedVersion:  0,
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
					ScheduledAtPriority: func() *int32 {
						scheduledAtPriority := int32(5)
						return &scheduledAtPriority
					}(),
				},
			},
			expectedUpdatedJobs: []*jobdb.Job{
				queuedJob.WithUpdatedRun(
					testfixtures.JobDb.CreateRun(
						uuid.UUID{},
						queuedJob.Id(),
						123,
						"test-executor",
						"test-executor-test-node",
						"test-node",
						&scheduledAtPriority,
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
			initialJobs: []*jobdb.Job{leasedJob},
			jobUpdates: []database.Job{
				{
					JobID:          leasedJob.Id(),
					JobSet:         leasedJob.Jobset(),
					Queue:          leasedJob.Queue(),
					Submitted:      leasedJob.Created(),
					Priority:       int64(leasedJob.Priority()),
					SchedulingInfo: schedulingInfoBytes,
					Succeeded:      true,
					Serial:         1,
				},
			},
			runUpdates: []database.Run{
				{
					RunID:     leasedJob.LatestRun().Id(),
					JobID:     leasedJob.LatestRun().JobId(),
					JobSet:    leasedJob.GetJobSet(),
					Succeeded: true,
				},
			},
			expectedUpdatedJobs: []*jobdb.Job{leasedJob.
				WithUpdatedRun(leasedJob.LatestRun().WithSucceeded(true)).
				WithSucceeded(true)},
			expectedJobDbIds: []string{},
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
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
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
			sched, err := NewScheduler(
				testfixtures.NewJobDb(),
				jobRepo,
				clusterRepo,
				schedulingAlgo,
				leaderController,
				publisher,
				nil,
				1*time.Second,
				5*time.Second,
				1*time.Hour,
				maxNumberOfAttempts,
				nodeIdLabel,
				schedulerMetrics,
				nil,
			)
			require.NoError(t, err)
			sched.EnableAssertions()

			// The SchedulingKeyGenerator embedded in the jobDb has some randomness,
			// which must be consistent within tests.
			sched.jobDb = testfixtures.NewJobDb()

			// insert initial jobs
			txn := sched.jobDb.WriteTxn()
			err = txn.Upsert(tc.initialJobs)
			require.NoError(t, err)
			txn.Commit()

			updatedJobs, _, _, err := sched.syncState(ctx)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedUpdatedJobs, updatedJobs)
			allDbJobs := sched.jobDb.ReadTxn().GetAll()

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

func (t *testJobRepository) FindInactiveRuns(ctx *armadacontext.Context, runIds []uuid.UUID) ([]uuid.UUID, error) {
	// TODO implement me
	panic("implement me")
}

func (t *testJobRepository) FetchJobRunLeases(ctx *armadacontext.Context, executor string, maxResults uint, excludedRunIds []uuid.UUID) ([]*database.JobRunLease, error) {
	// TODO implement me
	panic("implement me")
}

func (t *testJobRepository) FetchJobUpdates(ctx *armadacontext.Context, jobSerial int64, jobRunSerial int64) ([]database.Job, []database.Run, error) {
	if t.shouldError {
		return nil, nil, errors.New("error fetchiung job updates")
	}
	return t.updatedJobs, t.updatedRuns, nil
}

func (t *testJobRepository) FetchJobRunErrors(ctx *armadacontext.Context, runIds []uuid.UUID) (map[uuid.UUID]*armadaevents.Error, error) {
	if t.shouldError {
		return nil, errors.New("error fetching job run errors")
	}
	return t.errors, nil
}

func (t *testJobRepository) CountReceivedPartitions(ctx *armadacontext.Context, groupId uuid.UUID) (uint32, error) {
	if t.shouldError {
		return 0, errors.New("error counting received partitions")
	}
	return t.numReceivedPartitions, nil
}

type testExecutorRepository struct {
	updateTimes map[string]time.Time
	shouldError bool
}

func (t testExecutorRepository) GetExecutors(ctx *armadacontext.Context) ([]*schedulerobjects.Executor, error) {
	panic("not implemented")
}

func (t testExecutorRepository) GetLastUpdateTimes(ctx *armadacontext.Context) (map[string]time.Time, error) {
	if t.shouldError {
		return nil, errors.New("error getting last update time")
	}
	return t.updateTimes, nil
}

func (t testExecutorRepository) StoreExecutor(ctx *armadacontext.Context, executor *schedulerobjects.Executor) error {
	panic("not implemented")
}

type testSchedulingAlgo struct {
	numberOfScheduleCalls int
	jobsToPreempt         []string
	jobsToSchedule        []string
	jobsToFail            []string
	shouldError           bool
	// Set to true to indicate that preemption/scheduling/failure decisions have been persisted.
	// Until persisted is set to true, the same jobs are preempted/scheduled/failed on every call.
	persisted bool
}

func (t *testSchedulingAlgo) Schedule(_ *armadacontext.Context, txn *jobdb.Txn) (*SchedulerResult, error) {
	t.numberOfScheduleCalls++
	if t.shouldError {
		return nil, errors.New("error scheduling jobs")
	}
	if t.persisted {
		// Exit right away if decisions have already been persisted.
		return &SchedulerResult{}, nil
	}
	preemptedJobs := make([]*jobdb.Job, 0, len(t.jobsToPreempt))
	scheduledJobs := make([]*jobdb.Job, 0, len(t.jobsToSchedule))
	failedJobs := make([]*jobdb.Job, 0, len(t.jobsToFail))
	for _, id := range t.jobsToPreempt {
		job := txn.GetById(id)
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
		job := txn.GetById(id)
		if job == nil {
			return nil, errors.Errorf("was asked to lease %s but job does not exist", id)
		}
		if !job.Queued() {
			return nil, errors.Errorf("was asked to lease %s but job is not queued", job.Id())
		}
		priority := int32(0)
		if req := job.PodRequirements(); req != nil {
			priority = req.Priority
		}
		job = job.WithQueuedVersion(job.QueuedVersion()+1).WithQueued(false).WithNewRun(
			testExecutor,
			testNodeId,
			testNode,
			priority,
		)
		scheduledJobs = append(scheduledJobs, job)
	}
	for _, id := range t.jobsToFail {
		job := txn.GetById(id)
		if job == nil {
			return nil, errors.Errorf("was asked to lease %s but job does not exist", id)
		}
		if !job.Queued() {
			return nil, errors.Errorf("was asked to lease %s but job is not queued", job.Id())
		}
		job = job.WithQueued(false).WithFailed(true)
		failedJobs = append(failedJobs, job)
	}
	if err := txn.Upsert(preemptedJobs); err != nil {
		return nil, err
	}
	if err := txn.Upsert(scheduledJobs); err != nil {
		return nil, err
	}
	if err := txn.Upsert(failedJobs); err != nil {
		return nil, err
	}
	return NewSchedulerResultForTest(preemptedJobs, scheduledJobs, failedJobs, nil), nil
}

func (t *testSchedulingAlgo) Persist() {
	if t.numberOfScheduleCalls == 0 {
		// Nothing to persist if there have been no calls to schedule.
		return
	}
	t.persisted = true
	return
}

func NewSchedulerResultForTest[S ~[]T, T interfaces.LegacySchedulerJob](
	preemptedJobs S,
	scheduledJobs S,
	failedJobs S,
	nodeIdByJobId map[string]string,
) *SchedulerResult {
	return &SchedulerResult{
		PreemptedJobs: schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, preemptedJobs, GangIdAndCardinalityFromAnnotations),
		ScheduledJobs: schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, scheduledJobs, GangIdAndCardinalityFromAnnotations),
		FailedJobs:    schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, failedJobs, GangIdAndCardinalityFromAnnotations),
		NodeIdByJobId: nodeIdByJobId,
	}
}

type testPublisher struct {
	i              int
	eventSequences []*armadaevents.EventSequence
	shouldError    bool
}

func (t *testPublisher) PublishMessages(_ *armadacontext.Context, events []*armadaevents.EventSequence, shouldPublish func() bool) error {
	if t.shouldError {
		return errors.New("testPublisher error")
	}
	if shouldPublish() {
		t.eventSequences = append(t.eventSequences, events...)
	}
	return nil
}

func (t *testPublisher) ReadAll() []*armadaevents.EventSequence {
	if len(t.eventSequences) == 0 {
		return nil
	}
	rv := t.eventSequences[t.i:len(t.eventSequences)]
	t.i += len(rv)
	return rv
}

func (t *testPublisher) Reset() {
	t.eventSequences = nil
}

func (t *testPublisher) PublishMarkers(ctx *armadacontext.Context, groupId uuid.UUID) (uint32, error) {
	return 100, nil
}

func stringSet(src []string) map[string]bool {
	set := make(map[string]bool, len(src))
	for _, s := range src {
		set[s] = true
	}
	return set
}

var (
	newJobA = &database.Job{
		JobID:                 util.NewULID(),
		JobSet:                "testJobSet",
		Queue:                 "testQueue",
		Queued:                true,
		QueuedVersion:         0,
		SchedulingInfo:        schedulingInfoBytes,
		SchedulingInfoVersion: int32(schedulingInfo.Version),
		Serial:                0,
	}
	runningJobA = &database.Job{
		JobID:                 newJobA.JobID,
		JobSet:                "testJobSet",
		Queue:                 "testQueue",
		QueuedVersion:         1,
		SchedulingInfo:        schedulingInfoBytes,
		SchedulingInfoVersion: int32(schedulingInfo.Version),
		Serial:                0,
	}
	newRunA = &database.Run{
		RunID:               testfixtures.UUIDFromInt(1),
		JobID:               newJobA.JobID,
		JobSet:              newJobA.JobSet,
		Executor:            testExecutor,
		Node:                testNode,
		RunAttempted:        true,
		Serial:              0,
		ScheduledAtPriority: &scheduledAtPriority,
	}
	successfulRunA = &database.Run{
		RunID:               testfixtures.UUIDFromInt(1),
		JobID:               newJobA.JobID,
		JobSet:              newJobA.JobSet,
		Executor:            testExecutor,
		Node:                testNode,
		Succeeded:           true,
		RunAttempted:        true,
		Serial:              0,
		ScheduledAtPriority: &scheduledAtPriority,
	}
	newJobB = &database.Job{
		JobID:                 util.NewULID(),
		JobSet:                "testJobSet",
		Queue:                 "testQueue",
		Queued:                true,
		QueuedVersion:         0,
		SchedulingInfo:        schedulingInfoBytes,
		SchedulingInfoVersion: int32(schedulingInfo.Version),
		Serial:                1,
	}
)

// TestCycleConsistency runs two replicas of the scheduler and asserts that their state remains consistent
// under various permutations of making scheduling decisions and failovers.
func TestCycleConsistency(t *testing.T) {
	type schedulerDbUpdate struct {
		jobUpdates   []*database.Job              // Job updates from the database.
		runUpdates   []*database.Run              // Run updates from the database.
		jobRunErrors []*armadaevents.JobRunErrors // Job run errors from the database.
	}
	tests := map[string]struct {
		firstSchedulerDbUpdate  schedulerDbUpdate
		secondSchedulerDbUpdate schedulerDbUpdate

		initialJobs  []*jobdb.Job
		jobUpdates   []*database.Job              // Job updates from the database.
		runUpdates   []*database.Run              // Run updates from the database.
		jobRunErrors []*armadaevents.JobRunErrors // Job run errors from the database.

		idsOfJobsToSchedule []string
		idsOfJobsToPreempt  []string
		idsOfJobsToFail     []string

		// Expected jobDbs for scenario 1, i.e., the baseline scenario.
		// Only compared against if not nil.
		expectedBaselineJobDb *jobdb.JobDb

		expectedJobDbCycleOne   *jobdb.JobDb
		expectedJobDbCycleTwo   *jobdb.JobDb
		expectedJobDbCycleThree *jobdb.JobDb

		// Expected published events for scenario 1, i.e., the baseline scenario.
		// Only compared against if not nil.
		expectedEventSequencesCycleOne   []*armadaevents.EventSequence
		expectedEventSequencesCycleTwo   []*armadaevents.EventSequence
		expectedEventSequencesCycleThree []*armadaevents.EventSequence
	}{
		"Schedule a new job": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					newJobA,
				},
			},
			idsOfJobsToSchedule: []string{newJobA.JobID},
			expectedJobDbCycleOne: func() *jobdb.JobDb {
				jobDb := testfixtures.NewJobDb()
				job := jobDb.NewJob(
					newJobA.JobID,
					newJobA.JobSet,
					newJobA.Queue,
					uint32(newJobA.Priority),
					schedulingInfo,
					false,
					1,
					newJobA.CancelRequested,
					newJobA.CancelByJobsetRequested,
					newJobA.Cancelled,
					0,
				).WithNewRun(testExecutor, testNodeId, testNode, 10)
				txn := jobDb.WriteTxn()
				defer txn.Abort()
				if err := txn.Upsert([]*jobdb.Job{job}); err != nil {
					panic(err)
				}
				txn.Commit()
				return jobDb
			}(),
			expectedEventSequencesCycleThree: []*armadaevents.EventSequence{
				{
					Queue:      newJobA.Queue,
					JobSetName: newJobA.JobSet,
					Events: func() []*armadaevents.EventSequence_Event {
						uuidProvider := testfixtures.MockUUIDProvider{}
						runId := uuidProvider.New()
						created := time.Unix(0, 0)
						return []*armadaevents.EventSequence_Event{
							{
								Created: &created,
								Event: &armadaevents.EventSequence_Event_JobRunLeased{
									JobRunLeased: &armadaevents.JobRunLeased{
										RunId:                  armadaevents.ProtoUuidFromUuid(runId),
										JobId:                  armadaevents.MustProtoUuidFromUlidString(newJobA.JobID),
										ExecutorId:             testExecutor,
										NodeId:                 testNode,
										UpdateSequenceNumber:   1,
										HasScheduledAtPriority: true,
										ScheduledAtPriority:    10,
										AdditionalAnnotations:  make(map[string]string),
									},
								},
							},
						}
					}(),
				},
			},
		},
		"Schedule a new job loaded in the second cycle": {
			secondSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					newJobA,
				},
			},
			idsOfJobsToSchedule: []string{newJobA.JobID},
		},
		"Schedule two new jobs loaded in the second cycle": {
			secondSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					newJobA,
					newJobB,
				},
			},
			idsOfJobsToSchedule: []string{newJobA.JobID, newJobB.JobID},
		},
		"Schedule two new jobs loaded in separate cycles": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					newJobA,
				},
			},
			secondSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					newJobB,
				},
			},
			idsOfJobsToSchedule: []string{newJobA.JobID, newJobB.JobID},
		},
		"Schedule a new job that then succeeds": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					newJobA,
				},
			},
			secondSchedulerDbUpdate: schedulerDbUpdate{
				runUpdates: []*database.Run{
					successfulRunA,
				},
			},
			idsOfJobsToSchedule: []string{newJobA.JobID},
			expectedJobDbCycleOne: func() *jobdb.JobDb {
				jobDb := testfixtures.NewJobDb()
				job := jobDb.NewJob(
					newJobA.JobID,
					newJobA.JobSet,
					newJobA.Queue,
					uint32(newJobA.Priority),
					schedulingInfo,
					false,
					1,
					newJobA.CancelRequested,
					newJobA.CancelByJobsetRequested,
					newJobA.Cancelled,
					0,
				).WithNewRun(testExecutor, testNodeId, testNode, 10)
				txn := jobDb.WriteTxn()
				defer txn.Abort()
				if err := txn.Upsert([]*jobdb.Job{job}); err != nil {
					panic(err)
				}
				txn.Commit()
				return jobDb
			}(),
			expectedJobDbCycleTwo: func() *jobdb.JobDb {
				jobDb := testfixtures.NewJobDb()
				job := jobDb.NewJob(
					newJobA.JobID,
					newJobA.JobSet,
					newJobA.Queue,
					uint32(newJobA.Priority),
					schedulingInfo,
					false,
					1,
					newJobA.CancelRequested,
					newJobA.CancelByJobsetRequested,
					newJobA.Cancelled,
					0,
				).WithNewRun(testExecutor, testNodeId, testNode, 10).WithSucceeded(true)
				job = job.WithUpdatedRun(job.LatestRun().WithSucceeded(true).WithAttempted(true))
				txn := jobDb.WriteTxn()
				defer txn.Abort()
				if err := txn.Upsert([]*jobdb.Job{job}); err != nil {
					panic(err)
				}
				txn.Commit()
				return jobDb
			}(),
			expectedJobDbCycleThree: testfixtures.NewJobDb(),
			expectedEventSequencesCycleThree: []*armadaevents.EventSequence{
				{
					Queue:      newJobA.Queue,
					JobSetName: newJobA.JobSet,
					Events: func() []*armadaevents.EventSequence_Event {
						uuidProvider := testfixtures.MockUUIDProvider{}
						runId := uuidProvider.New()
						created := time.Unix(0, 0)
						return []*armadaevents.EventSequence_Event{
							{
								Created: &created,
								Event: &armadaevents.EventSequence_Event_JobRunLeased{
									JobRunLeased: &armadaevents.JobRunLeased{
										RunId:                  armadaevents.ProtoUuidFromUuid(runId),
										JobId:                  armadaevents.MustProtoUuidFromUlidString(newJobA.JobID),
										ExecutorId:             testExecutor,
										NodeId:                 testNode,
										UpdateSequenceNumber:   1,
										HasScheduledAtPriority: true,
										ScheduledAtPriority:    10,
										AdditionalAnnotations:  make(map[string]string),
									},
								},
							},
						}
					}(),
				},
				{
					Queue:      newJobA.Queue,
					JobSetName: newJobA.JobSet,
					Events: func() []*armadaevents.EventSequence_Event {
						created := time.Unix(0, 0)
						return []*armadaevents.EventSequence_Event{
							{
								Created: &created,
								Event: &armadaevents.EventSequence_Event_JobSucceeded{
									JobSucceeded: &armadaevents.JobSucceeded{
										JobId: armadaevents.MustProtoUuidFromUlidString(newJobA.JobID),
									},
								},
							},
						}
					}(),
				},
			},
		},
		"Running job is preempted": {
			firstSchedulerDbUpdate: schedulerDbUpdate{
				jobUpdates: []*database.Job{
					runningJobA,
				},
				runUpdates: []*database.Run{
					newRunA,
				},
			},
			idsOfJobsToPreempt:      []string{newJobA.JobID},
			expectedJobDbCycleThree: testfixtures.NewJobDb(),
			expectedEventSequencesCycleThree: []*armadaevents.EventSequence{
				{
					Queue:      newJobA.Queue,
					JobSetName: newJobA.JobSet,
					Events: []*armadaevents.EventSequence_Event{
						{
							Created: pointerFromValue(time.Unix(0, 0)),
							Event: &armadaevents.EventSequence_Event_JobRunPreempted{
								JobRunPreempted: &armadaevents.JobRunPreempted{
									PreemptedRunId: armadaevents.ProtoUuidFromUuid(testfixtures.UUIDFromInt(1)),
									PreemptedJobId: armadaevents.MustProtoUuidFromUlidString(newJobA.JobID),
								},
							},
						},
						{
							Created: pointerFromValue(time.Unix(0, 0)),
							Event: &armadaevents.EventSequence_Event_JobRunErrors{
								JobRunErrors: &armadaevents.JobRunErrors{
									JobId: armadaevents.MustProtoUuidFromUlidString(newJobA.JobID),
									RunId: armadaevents.ProtoUuidFromUuid(testfixtures.UUIDFromInt(1)),
									Errors: []*armadaevents.Error{
										{
											Terminal: true,
											Reason: &armadaevents.Error_JobRunPreemptedError{
												JobRunPreemptedError: &armadaevents.JobRunPreemptedError{},
											},
										},
									},
								},
							},
						},
						{
							Created: pointerFromValue(time.Unix(0, 0)),
							Event: &armadaevents.EventSequence_Event_JobErrors{
								JobErrors: &armadaevents.JobErrors{
									JobId: armadaevents.MustProtoUuidFromUlidString(newJobA.JobID),
									Errors: []*armadaevents.Error{
										{
											Terminal: true,
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
		"Job succeeded": {
			jobUpdates: []*database.Job{
				{
					JobID:                 leasedJob.Id(),
					JobSet:                "testJobSet",
					Queue:                 "testQueue",
					Queued:                false,
					QueuedVersion:         1,
					SchedulingInfo:        schedulingInfoBytes,
					SchedulingInfoVersion: int32(schedulingInfo.Version),
					Serial:                0,
				},
			},
			runUpdates: []*database.Run{
				{
					RunID:     leasedJob.LatestRun().Id(),
					JobID:     leasedJob.Id(),
					Node:      "testNode",
					JobSet:    "testJobSet",
					Executor:  "testExecutor",
					Succeeded: true,
					Serial:    0,
				},
			},
		},
		"New failed job with no runs": {
			// This happens if the scheduler decides to fail a job, e.g., due to min-max gang scheduling.
			jobUpdates: []*database.Job{
				{
					JobID:                 queuedJob.Id(),
					JobSet:                "testJobSet",
					Queue:                 "testQueue",
					Failed:                true,
					QueuedVersion:         0,
					SchedulingInfo:        schedulingInfoBytes,
					SchedulingInfoVersion: int32(schedulingInfo.Version),
					Serial:                1,
				},
			},
			// expectedQueuedVersion: 0,
		},
		"Queued job transitions straight to failed without running": {
			// This happens if the scheduler decides to fail a job, e.g., due to min-max gang scheduling.
			initialJobs: []*jobdb.Job{queuedJob},
			jobUpdates: []*database.Job{
				{
					JobID:                 queuedJob.Id(),
					JobSet:                "testJobSet",
					Queue:                 "testQueue",
					Failed:                true,
					QueuedVersion:         0,
					SchedulingInfo:        schedulingInfoBytes,
					SchedulingInfoVersion: int32(schedulingInfo.Version),
					Serial:                1,
				},
			},
			// expectedQueuedVersion: 0,
		},
		"Nothing leased": {
			initialJobs: []*jobdb.Job{queuedJob},
			// expectedQueued:        []string{queuedJob.Id()},
			// expectedQueuedVersion: queuedJob.QueuedVersion(),
		},
		"FailedJobs in scheduler result will publish appropriate messages": {
			initialJobs: []*jobdb.Job{queuedJob},
			// expectedJobErrors:  []string{queuedJob.Id()},
			// expectedJobsToFail: []string{queuedJob.Id()},
			// expectedTerminal:   []string{queuedJob.Id()},
		},
		"No updates to an already leased job": {
			initialJobs: []*jobdb.Job{leasedJob},
			// expectedLeased:        []string{leasedJob.Id()},
			// expectedQueuedVersion: leasedJob.QueuedVersion(),
		},
		"No updates to a requeued job already db": {
			initialJobs: []*jobdb.Job{requeuedJob},
			// expectedQueued:        []string{requeuedJob.Id()},
			// expectedQueuedVersion: requeuedJob.QueuedVersion(),
		},
		"No updates to a requeued job from update": {
			jobUpdates: []*database.Job{
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
			runUpdates: []*database.Run{
				{
					RunID:        requeuedJob.LatestRun().Id(),
					JobID:        requeuedJob.Id(),
					JobSet:       "testJobSet",
					Executor:     "testExecutor",
					Node:         "node",
					Failed:       true,
					Returned:     true,
					RunAttempted: true,
					Serial:       1,
				},
			},
			// expectedQueued:        []string{requeuedJob.Id()},
			// expectedQueuedVersion: requeuedJob.QueuedVersion(),
		},
		"Lease returned and re-queued when run attempted": {
			initialJobs: []*jobdb.Job{leasedJob},
			runUpdates: []*database.Run{
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
			jobRunErrors: []*armadaevents.JobRunErrors{
				defaultJobRunError(leasedJob.Id(), leasedJob.LatestRun().Id()),
			},
			// This one fails bc it goes to look for a jobRunErrors it can't find.
			//
			//
			// expectedQueued:   []string{leasedJob.Id()},
			// expectedRequeued: []string{leasedJob.Id()},
			// // Should add node anti affinities for nodes of any attempted runs
			// expectedNodeAntiAffinities:       []string{leasedJob.LatestRun().NodeName()},
			// expectedJobSchedulingInfoVersion: 2,
			// expectedQueuedVersion:            leasedJob.QueuedVersion() + 1,
		},
		"Lease returned and re-queued when run not attempted": {
			initialJobs: []*jobdb.Job{leasedJob},
			runUpdates: []*database.Run{
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
			// expectedQueued:        []string{leasedJob.Id()},
			// expectedRequeued:      []string{leasedJob.Id()},
			// expectedQueuedVersion: leasedJob.QueuedVersion() + 1,
		},
		// // When a lease is returned and the run was attempted, a node anti affinity is added
		// // If this node anti-affinity makes the job unschedulable, it should be failed
		// "Lease returned and failed": {
		// 	initialJobs: []*jobdb.Job{leasedJob},
		// 	runUpdates: []*database.Run{
		// 		{
		// 			RunID:        leasedJob.LatestRun().Id(),
		// 			JobID:        leasedJob.Id(),
		// 			JobSet:       "testJobSet",
		// 			Executor:     "testExecutor",
		// 			Failed:       true,
		// 			Returned:     true,
		// 			RunAttempted: true,
		// 			Serial:       1,
		// 		},
		// 	},
		// 	submitCheckerFailure:  true,
		// 	expectedJobErrors:     []string{leasedJob.Id()},
		// 	expectedTerminal:      []string{leasedJob.Id()},
		// 	expectedQueuedVersion: leasedJob.QueuedVersion(),
		// },
		"Lease returned too many times": {
			initialJobs: []*jobdb.Job{returnedOnceLeasedJob},
			runUpdates: []*database.Run{
				{
					RunID:        returnedOnceLeasedJob.LatestRun().Id(),
					JobID:        returnedOnceLeasedJob.Id(),
					JobSet:       "testJobSet",
					Executor:     "testExecutor",
					Node:         "testNode",
					Failed:       true,
					Returned:     true,
					RunAttempted: true,
					Serial:       2,
				},
			},
			// expectedJobErrors:     []string{returnedOnceLeasedJob.Id()},
			// expectedTerminal:      []string{returnedOnceLeasedJob.Id()},
			// expectedQueuedVersion: 1,
		},
		"Lease returned for fail fast job": {
			initialJobs: []*jobdb.Job{leasedFailFastJob},
			// Fail fast should mean there is only ever 1 attempted run
			runUpdates: []*database.Run{
				{
					RunID:        leasedFailFastJob.LatestRun().Id(),
					JobID:        leasedFailFastJob.Id(),
					JobSet:       "testJobSet",
					Executor:     "testExecutor",
					Failed:       true,
					Returned:     true,
					RunAttempted: false,
					Serial:       1,
				},
			},
			// expectedJobErrors:     []string{leasedFailFastJob.Id()},
			// expectedTerminal:      []string{leasedFailFastJob.Id()},
			// expectedQueuedVersion: leasedFailFastJob.QueuedVersion(),
		},
		"Job cancelled": {
			initialJobs: []*jobdb.Job{leasedJob},
			jobUpdates: []*database.Job{
				{
					JobID:           leasedJob.Id(),
					JobSet:          "testJobSet",
					Queue:           "testQueue",
					SchedulingInfo:  schedulingInfoBytes,
					CancelRequested: true,
					Serial:          1,
				},
			},
			// expectedJobCancelled:  []string{leasedJob.Id()},
			// expectedTerminal:      []string{leasedJob.Id()},
			// expectedQueuedVersion: leasedJob.QueuedVersion(),
		},
		"New job from postgres with expired queue ttl is cancel requested": {
			jobUpdates: []*database.Job{
				{
					JobID:          queuedJobWithExpiredTtl.Id(),
					JobSet:         queuedJobWithExpiredTtl.Jobset(),
					Queue:          queuedJobWithExpiredTtl.Queue(),
					Queued:         queuedJobWithExpiredTtl.Queued(),
					QueuedVersion:  queuedJobWithExpiredTtl.QueuedVersion(),
					Serial:         1,
					Submitted:      queuedJobWithExpiredTtl.Created(),
					SchedulingInfo: schedulingInfoWithQueueTtlBytes,
				},
			},

			// // We expect to publish request cancel and cancelled message this cycle.
			// // The job should also be removed from the queue and set to a terminal state.
			// expectedJobRequestCancel: []string{queuedJobWithExpiredTtl.Id()},
			// expectedJobCancelled:     []string{queuedJobWithExpiredTtl.Id()},
			// expectedQueuedVersion:    queuedJobWithExpiredTtl.QueuedVersion(),
			// expectedTerminal:         []string{queuedJobWithExpiredTtl.Id()},
		},
		"Existing jobDb job with expired queue ttl is cancel requested": {
			initialJobs: []*jobdb.Job{queuedJobWithExpiredTtl},

			// // We expect to publish request cancel and cancelled message this cycle.
			// // The job should also be removed from the queue and set to a terminal state.
			// expectedJobRequestCancel: []string{queuedJobWithExpiredTtl.Id()},
			// expectedJobCancelled:     []string{queuedJobWithExpiredTtl.Id()},
			// expectedQueuedVersion:    queuedJobWithExpiredTtl.QueuedVersion(),
			// expectedTerminal:         []string{queuedJobWithExpiredTtl.Id()},
		},
		"New postgres job with cancel requested results in cancel messages": {
			jobUpdates: []*database.Job{
				{
					JobID:           queuedJobWithExpiredTtl.Id(),
					JobSet:          queuedJobWithExpiredTtl.Jobset(),
					Queue:           queuedJobWithExpiredTtl.Queue(),
					Queued:          queuedJobWithExpiredTtl.Queued(),
					QueuedVersion:   queuedJobWithExpiredTtl.QueuedVersion(),
					Serial:          1,
					Submitted:       queuedJobWithExpiredTtl.Created(),
					CancelRequested: true,
					Cancelled:       false,
					SchedulingInfo:  schedulingInfoWithQueueTtlBytes,
				},
			},

			// // We have already got a request cancel from the DB, so only publish a cancelled message.
			// // The job should also be removed from the queue and set to a terminal state.#
			// expectedJobCancelled:  []string{queuedJobWithExpiredTtl.Id()},
			// expectedQueuedVersion: queuedJobWithExpiredTtl.QueuedVersion(),
			// expectedTerminal:      []string{queuedJobWithExpiredTtl.Id()},
		},
		"Postgres job with cancel requested results in cancel messages": {
			initialJobs: []*jobdb.Job{queuedJobWithExpiredTtl.WithCancelRequested(true)},
			jobUpdates: []*database.Job{
				{
					JobID:           queuedJobWithExpiredTtl.Id(),
					JobSet:          queuedJobWithExpiredTtl.Jobset(),
					Queue:           queuedJobWithExpiredTtl.Queue(),
					Queued:          queuedJobWithExpiredTtl.Queued(),
					QueuedVersion:   queuedJobWithExpiredTtl.QueuedVersion(),
					Serial:          1,
					Submitted:       queuedJobWithExpiredTtl.Created(),
					CancelRequested: true,
					Cancelled:       false,
					SchedulingInfo:  schedulingInfoWithQueueTtlBytes,
				},
			},

			// // We have already got a request cancel from the DB/existing job state, so only publish a cancelled message.
			// // The job should also be removed from the queue and set to a terminal state.
			// expectedJobCancelled:  []string{queuedJobWithExpiredTtl.Id()},
			// expectedQueuedVersion: queuedJobWithExpiredTtl.QueuedVersion(),
			// expectedTerminal:      []string{queuedJobWithExpiredTtl.Id()},
		},
		"Job reprioritised": {
			initialJobs: []*jobdb.Job{queuedJob},
			jobUpdates: []*database.Job{
				{
					JobID:          queuedJob.Id(),
					JobSet:         "testJobSet",
					Queue:          "testQueue",
					SchedulingInfo: schedulingInfoBytes,
					Priority:       2,
					Serial:         1,
				},
			},
			// expectedJobReprioritised: []string{queuedJob.Id()},
			// expectedQueued:           []string{queuedJob.Id()},
			// expectedJobPriority:      map[string]uint32{queuedJob.Id(): 2},
			// expectedQueuedVersion:    queuedJob.QueuedVersion(),
		},
		"Lease expired": {
			initialJobs: []*jobdb.Job{leasedJob},
			// staleExecutor:         true,
			// expectedJobRunErrors:  []string{leasedJob.Id()},
			// expectedJobErrors:     []string{leasedJob.Id()},
			// expectedTerminal:      []string{leasedJob.Id()},
			// expectedQueuedVersion: leasedJob.QueuedVersion(),
		},
		"Job failed": {
			initialJobs: []*jobdb.Job{leasedJob},
			runUpdates: []*database.Run{
				{
					RunID:    leasedJob.LatestRun().Id(),
					JobID:    leasedJob.Id(),
					JobSet:   "testJobSet",
					Executor: "testExecutor",
					Failed:   true,
					Serial:   1,
				},
			},
			jobRunErrors: []*armadaevents.JobRunErrors{
				defaultJobRunError(leasedJob.Id(), leasedJob.LatestRun().Id()),
			},
			// expectedJobErrors:     []string{leasedJob.Id()},
			// expectedTerminal:      []string{leasedJob.Id()},
			// expectedQueuedVersion: leasedJob.QueuedVersion(),
		},
		"Job preempted": {
			initialJobs:        []*jobdb.Job{leasedJob},
			idsOfJobsToPreempt: []string{leasedJob.Id()},
			// expectedJobRunPreempted: []string{leasedJob.Id()},
			// expectedJobErrors:       []string{leasedJob.Id()},
			// expectedJobRunErrors:    []string{leasedJob.Id()},
			// expectedTerminal:        []string{leasedJob.Id()},
			// expectedQueuedVersion:   leasedJob.QueuedVersion(),
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
			instructionConverter, err := scheduleringester.NewInstructionConverter(
				nil,
				testfixtures.TestPriorityClasses,
			)
			require.NoError(t, err)

			// Helper function for creating new schedulers for use in tests.
			newScheduler := func(db *pgxpool.Pool) *Scheduler {
				scheduler, err := NewScheduler(
					testfixtures.NewJobDb(),
					database.NewPostgresJobRepository(db, 1024),
					&testExecutorRepository{
						updateTimes: map[string]time.Time{"test-executor": testClock.Now()},
					},
					&testSchedulingAlgo{
						jobsToSchedule: tc.idsOfJobsToSchedule,
						jobsToPreempt:  tc.idsOfJobsToPreempt,
						jobsToFail:     tc.idsOfJobsToFail,
					},
					NewStandaloneLeaderController(),
					&testPublisher{},
					&testSubmitChecker{},
					1*time.Second,
					5*time.Second,
					0,
					maxNumberOfAttempts,
					nodeIdLabel,
					schedulerMetrics,
					nil,
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

					// Create two scheduler using the same db connection.
					a := newScheduler(db)
					b := newScheduler(db)

					// Share the schedulingAlgo to ensure both schedulers make the same scheduling decisions.
					b.schedulingAlgo = a.schedulingAlgo

					// Initially, "a" is leader and "b" follower.
					(b.leaderController.(*StandaloneLeaderController)).SetToken(InvalidLeaderToken())

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
				_, err := s.cycle(ctx, updateAll, s.leaderController.GetToken(), shouldSchedule)
				return err
			}

			// persist persists to the schedulerDb any published eventSequences.
			persist := func(s *Scheduler, schedulerDb *scheduleringester.SchedulerDb) error {
				publisher := s.publisher.(*testPublisher)
				eventSequences := publisher.ReadAll()
				dbOpsWithMessageIds := instructionConverter.Convert(ctx, &ingest.EventSequencesWithIds{
					EventSequences: eventSequences,
				})

				// Mark scheduling decisions as persisted.
				// If not persisted, the same jobs are scheduled again on subsequent calls to schedule.
				s.schedulingAlgo.(*testSchedulingAlgo).Persist()

				// Logging
				numEvents := 0
				for _, eventSequence := range eventSequences {
					numEvents += len(eventSequence.Events)
				}
				t.Logf("persist scheduler %p; %d eventSequences, %d eventSequences, %d ops", s, len(eventSequences), numEvents, len(dbOpsWithMessageIds.Ops))
				for _, eventSequence := range eventSequences {
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
				lca := a.leaderController.(*StandaloneLeaderController)
				lcb := b.leaderController.(*StandaloneLeaderController)
				lta := lca.GetToken()
				ltb := lcb.GetToken()
				lca.SetToken(ltb)
				lcb.SetToken(lta)
				return nil
			}

			eventsFromTestPublisher := func(p Publisher) []*armadaevents.EventSequence {
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
				require.NoError(t, tc.expectedJobDbCycleOne.ReadTxn().AssertEqual(jobDbCycleOne.ReadTxn()), "unexpected cycle one jobDb")
			}
			if tc.expectedJobDbCycleTwo != nil {
				require.NoError(t, tc.expectedJobDbCycleTwo.ReadTxn().AssertEqual(jobDbCycleTwo.ReadTxn()), "unexpected cycle two jobDb")
			}
			if tc.expectedJobDbCycleThree != nil {
				require.NoError(t, tc.expectedJobDbCycleThree.ReadTxn().AssertEqual(jobDbCycleThree.ReadTxn()), "unexpected cycle three jobDb")
			}

			if tc.expectedEventSequencesCycleOne != nil {
				require.Equal(t, tc.expectedEventSequencesCycleOne, eventsCycleOne, "unexpected cycle one events")
			}
			if tc.expectedEventSequencesCycleTwo != nil {
				require.Equal(t, tc.expectedEventSequencesCycleTwo, eventsCycleTwo, "unexpected cycle two events")
			}
			if tc.expectedEventSequencesCycleThree != nil {
				require.Equal(t, tc.expectedEventSequencesCycleThree, eventsCycleThree, "unexpected cycle three events")
			}

			// Test that the follower stays in sync with the leader.
			// TODO(albin): We need to cycle "a" again after each persist for now.
			//              The jobDb of the leader is supposed to be updated immediately to reflect published events.
			//              However, the jobDb is only updated immediately for schedule/preempt/fail decisions.
			//              For external changes, e.g., job succeeded, the jobDb is only updated on the second cycle.
			t.Log("scenario 2")
			require.NoError(t, withTestSetup(func(a, b *Scheduler, schedulerDb *scheduleringester.SchedulerDb) error {
				require.NoError(t, firstDbUpdate(schedulerDb))
				require.NoError(t, cycle(a, false, true))
				require.NoError(t, persist(a, schedulerDb))
				require.NoError(t, cycle(a, false, true))
				require.NoError(t, cycle(b, false, false))
				require.NoError(t, a.jobDb.ReadTxn().AssertEqual(b.jobDb.ReadTxn()))
				require.Nil(t, eventsFromTestPublisher(b.publisher))

				require.NoError(t, secondDbUpdate(schedulerDb))
				require.NoError(t, cycle(a, false, true))
				require.NoError(t, persist(a, schedulerDb))
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

				require.Nil(t, eventsFromTestPublisher(b.publisher))
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
	instructionConverter *scheduleringester.InstructionConverter,
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
		insertJobsDbOp[dbJob.JobID] = dbJob
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
		jobId, err := armadaevents.UlidStringFromProtoUuid(jobRunError.JobId)
		if err != nil {
			return nil, err
		}
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
		&ingest.EventSequencesWithIds{
			EventSequences: jobRunErrorsEventSequences,
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
		job.SubmitMessage = make([]byte, 0)
	}
	return dbOp
}

func pointerFromValue[T any](v T) *T {
	return &v
}
