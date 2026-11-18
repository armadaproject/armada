package replay_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	log "github.com/armadaproject/armada/internal/common/logging"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/common/types"
	corescheduler "github.com/armadaproject/armada/internal/scheduler"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/priorityoverride"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	"github.com/armadaproject/armada/internal/scheduler/simulator/replay"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

func TestProductionSchedulerPreemptsAndResumesOfferedWork(t *testing.T) {
	const minute = int64(time.Minute / time.Microsecond)
	origin := testfixtures.BaseTime.UnixMicro()
	at := func(offset int64) int64 { return origin + offset*minute }

	cpu, gpu := resource.MustParse("1"), resource.MustParse("1")
	node := testfixtures.TestSchedulerObjectsNode(testfixtures.TestPriorities, map[string]*resource.Quantity{
		"cpu": &cpu, "nvidia.com/gpu": &gpu,
	})
	node.Id, node.Name, node.Executor, node.Pool = "node", "node", "executor", testfixtures.TestPool
	executors := []*schedulerobjects.Executor{{
		Id: node.Executor, Pool: node.Pool, Nodes: []*schedulerobjects.Node{node},
	}}
	cfg := testfixtures.TestSchedulingConfig()
	cfg.PriorityClasses = map[string]types.PriorityClass{
		testfixtures.PriorityClass1: {Priority: 1, Preemptible: true},
		testfixtures.PriorityClass3: {Priority: 3, Preemptible: true},
	}
	cfg.DefaultPriorityClassName = testfixtures.PriorityClass1
	cfg.MaxSchedulingDuration = time.Second
	cfg.NodeIdLabel = configuration.NodeIdLabel
	queue := testfixtures.MakeTestQueue()
	ctx := context.Background()
	scheduler, err := replay.NewProductionScheduler(ctx, at(-90), cfg, executors, []*api.Queue{queue})
	require.NoError(t, err)

	job := func(id, priority string, offset, work int64, requests v1.ResourceList) replay.Job {
		return replay.Job{
			ID: id, Queue: queue.Name, PriorityClass: priority, SubmittedUS: at(offset),
			UsefulUS: work * minute, Requests: requests,
		}
	}
	cpuJob := job("cpu", testfixtures.PriorityClass1, -90, 180, v1.ResourceList{"cpu": cpu})
	gpuJob := job("gpu", testfixtures.PriorityClass1, -90, 180, v1.ResourceList{"nvidia.com/gpu": gpu})
	cpuJob.StartupUS, gpuJob.StartupUS, gpuJob.CheckpointUS = 45*minute, 45*minute, 60*minute
	preemptor := job("preemptor", testfixtures.PriorityClass3, 30, 10, v1.ResourceList{"cpu": cpu, "nvidia.com/gpu": gpu})

	out := &observations{}
	_, err = replay.Run(ctx, replay.Window{
		WarmupUS: at(-90), StartUS: at(0), EndUS: at(210), CycleUS: minute,
	}, readJobs([]replay.Job{cpuJob, gpuJob, preemptor}), scheduler, out)
	require.NoError(t, err)

	type attempt struct {
		job                                  string
		ordinal, start, end                  int64
		state                                replay.State
		startup, retained, lost, provisional int64
	}
	actual := make([]attempt, 0, len(out.attempts))
	for _, row := range out.attempts {
		require.Equal(t, "node", row.Placement.Node)
		require.NotEmpty(t, row.Placement.RunID)
		actual = append(actual, attempt{
			row.JobID, row.Ordinal, (row.AllocatedUS - origin) / minute,
			(row.EndUS - origin) / minute, row.State, row.StartupUS / minute,
			row.RetainedUS / minute, row.LostUS / minute, row.ProvisionalUS / minute,
		})
	}
	require.Equal(t, []attempt{
		{"cpu", 1, -90, 30, replay.Preempted, 45, 0, 75, 0},
		{"gpu", 1, -90, 30, replay.Preempted, 45, 60, 15, 0},
		{"preemptor", 1, 30, 40, replay.Completed, 0, 10, 0, 0},
		{"gpu", 2, 40, 205, replay.Completed, 45, 120, 0, 0},
		{"cpu", 2, 40, 210, replay.Running, 45, 0, 0, 125},
	}, actual)

	reference := productionTimeline(t, cfg, node, queue, at, cpuJob, gpuJob, preemptor)
	replayRounds := make(map[int64]replay.Decisions)
	for _, row := range out.attempts {
		placement := row.Placement
		placement.RunID = ""
		round := replayRounds[row.AllocatedUS]
		round.Allocate = append(round.Allocate, placement)
		replayRounds[row.AllocatedUS] = round
		if row.State == replay.Preempted {
			round := replayRounds[row.EndUS]
			round.Preempt = append(round.Preempt, replay.Preemption{JobID: row.JobID, Reason: row.Reason})
			replayRounds[row.EndUS] = round
		}
	}
	for when, expected := range reference {
		replayed := replayRounds[when]
		require.ElementsMatch(t, expected.Preempt, replayed.Preempt, "preemptions at %d", when)
		require.ElementsMatch(t, expected.Allocate, replayed.Allocate, "allocations at %d", when)
	}
}

// The reference builds its own JobDb and executor state, drives the production
// submit checker and scheduler, and applies lifecycle inputs between rounds.
// Allocation, completion and retry timing are supplied by the fixture, not
// derived from replay's attempt accountant.
func productionTimeline(
	t *testing.T,
	cfg configuration.SchedulingConfig,
	source *schedulerobjects.Node,
	queue *api.Queue,
	at func(int64) int64,
	jobs ...replay.Job,
) map[int64]replay.Decisions {
	t.Helper()
	logger := armadacontext.New(t.Context(), log.NullLogger)
	clock := clocktesting.NewFakeClock(time.UnixMicro(at(-90)))
	node := *source
	node.StateByJobRunId = make(map[string]schedulerobjects.JobRunState)
	executor := &schedulerobjects.Executor{Id: node.Executor, Pool: node.Pool, Nodes: []*schedulerobjects.Node{&node}}
	repo := schedulermocks.NewMockExecutorRepository(gomock.NewController(t))
	repo.EXPECT().GetExecutors(gomock.Any()).Return([]*schedulerobjects.Executor{executor}, nil).AnyTimes()
	repo.EXPECT().GetExecutorSettings(gomock.Any()).Return(nil, nil).AnyTimes()
	cache := schedulermocks.NewMockQueueCache(gomock.NewController(t))
	cache.EXPECT().GetAll(gomock.Any()).Return([]*api.Queue{queue}, nil).AnyTimes()
	factory, err := internaltypes.NewResourceListFactory(cfg.SupportedResourceTypes, nil)
	require.NoError(t, err)
	floating, err := floatingresources.NewFloatingResourceTypes(nil, factory)
	require.NoError(t, err)
	db := jobdb.NewJobDb(cfg.PriorityClasses, cfg.DefaultPriorityClassName, stringinterner.New(32), factory)
	db.SetClock(clock)
	checker := corescheduler.NewSubmitChecker(cfg, configuration.SubmitCheckConfig{}, repo, cache, floating, factory)
	require.NoError(t, checker.Initialise(logger))
	algo, err := scheduling.NewFairSchedulingAlgo(cfg, 0, repo, cache, nil, factory, floating,
		priorityoverride.NewNoOpProvider(), nil, scheduling.NewRunNodeReconciler(cfg.Pools), scheduling.WithPolicyClock(clock))
	require.NoError(t, err)

	admit := func(tx *jobdb.Txn, input replay.Job) {
		t.Helper()
		created := time.UnixMicro(input.SubmittedUS)
		info := &internaltypes.JobSchedulingInfo{
			PriorityClass: input.PriorityClass, SubmitTime: created,
			PodRequirements: &internaltypes.PodRequirements{ResourceRequirements: v1.ResourceRequirements{Requests: input.Requests}},
		}
		job, err := db.NewJob(input.ID, input.JobSet, input.Queue, input.QueuePriority, info,
			true, 0, false, false, false, created.UnixNano(), true, nil, 0)
		require.NoError(t, err)
		pools, _, err := checker.CheckJobEligibility(logger, job)
		require.NoError(t, err)
		require.NotEmpty(t, pools)
		require.NoError(t, tx.Upsert([]*jobdb.Job{job.WithPools(pools)}))
	}
	timeline := make(map[int64]replay.Decisions)
	for _, minute := range []int64{-90, -89, 30, 40, 205} {
		now := time.UnixMicro(at(minute))
		clock.SetTime(now)
		executor.LastUpdateTime = protoutil.ToTimestamp(now)
		txn := db.WriteTxn()
		for _, input := range jobs {
			if input.SubmittedUS == at(minute) {
				admit(txn, input)
			}
		}
		var completedID string
		switch minute {
		case 40:
			completedID = "preemptor"
		case 205:
			completedID = "gpu"
		}
		if completedID != "" {
			job := txn.GetById(completedID)
			require.NotNil(t, job)
			run := job.LatestRun().WithSucceeded(true).WithTerminatedTime(&now)
			require.NoError(t, txn.Upsert([]*jobdb.Job{job.WithUpdatedRun(run).WithSucceeded(true)}))
			delete(node.StateByJobRunId, run.Id())
		}
		result, err := algo.Schedule(logger, txn)
		require.NoError(t, err)
		var decisions replay.Decisions
		for _, pool := range result.PoolResults {
			require.NoError(t, pool.Outcome.Error())
		}
		for _, j := range result.GetAllPreemptedJobs() {
			job := j.Job
			delete(node.StateByJobRunId, job.LatestRun().Id())
			require.NoError(t, txn.Upsert([]*jobdb.Job{job.WithoutTerminal().WithQueued(true)}))
			decisions.Preempt = append(decisions.Preempt, replay.Preemption{JobID: job.Id(), Reason: string(j.PreemptionType)})
		}
		for _, j := range result.GetAllScheduledJobs() {
			run := j.Job.LatestRun()
			require.NotNil(t, run)
			require.Equal(t, now.UnixNano(), run.Created())
			require.True(t, now.Equal(*run.LeaseTime()))
			run = run.WithLeased(true).WithRunning(true).WithRunningTime(&now)
			require.NoError(t, txn.Upsert([]*jobdb.Job{j.Job.WithUpdatedRun(run)}))
			node.StateByJobRunId[run.Id()] = schedulerobjects.JobRunState_RUNNING
			decisions.Allocate = append(decisions.Allocate, replay.Placement{
				JobID: j.JobId, Node: run.NodeId(), Pool: node.Pool, SchedulingPool: run.Pool(),
			})
		}
		txn.Commit()
		timeline[at(minute)] = decisions
	}
	return timeline
}
