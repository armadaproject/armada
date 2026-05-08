package scheduler

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clock "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/leader"
	"github.com/armadaproject/armada/internal/scheduler/pricing"
	"github.com/armadaproject/armada/internal/scheduler/retry"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// fakePolicyCache backs the retry-policy lookup with a fixed in-memory map.
// Tests use this to drive the engine's failure-handling path without going
// through the gRPC cache implementation.
type fakePolicyCache map[string]*retry.Policy

func (c fakePolicyCache) Get(name string) (*retry.Policy, bool) {
	if p, ok := c[name]; ok {
		return p, true
	}
	return nil, false
}

// makeRetryTestScheduler builds a Scheduler wired with the bare minimum
// dependencies needed to exercise the failure path through
// generateUpdateMessagesFromJob. The legacy maxAttemptedRuns and the queue
// cache are still needed even with the feature flag off, so we set them.
func makeRetryTestScheduler(t *testing.T, ffEnabled bool, policyCache retry.PolicyCache) *Scheduler {
	return makeRetryTestSchedulerWithGlobalMax(t, ffEnabled, policyCache, 0)
}

// makeRetryTestSchedulerWithGlobalMax is the same as makeRetryTestScheduler
// but lets the caller set the global retry cap, used to pin the contract
// that the cap counts failed runs only.
func makeRetryTestSchedulerWithGlobalMax(t *testing.T, ffEnabled bool, policyCache retry.PolicyCache, globalMaxRetries uint) *Scheduler {
	t.Helper()
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	queueCache := &testQueueCache{queues: []*api.Queue{
		{Name: "testQueue", RetryPolicy: "test-policy"},
	}}

	sched, err := NewScheduler(
		jobDb,
		&testJobRepository{},
		&testExecutorRepository{},
		&testSchedulingAlgo{},
		leader.NewStandaloneLeaderController(),
		newTestPublisher(),
		&testSubmitChecker{checkSuccess: true},
		&testGangValidator{validateSuccess: true},
		1*time.Second,
		5*time.Second,
		1*time.Hour,
		nil,
		maxNumberOfAttempts,
		nodeIdLabel,
		schedulerMetrics,
		pricing.NoopBidPriceProvider{},
		[]string{},
		queueCache,
		schedulerconfig.RetryPolicyConfig{Enabled: ffEnabled, GlobalMaxRetries: globalMaxRetries},
		policyCache,
	)
	require.NoError(t, err)
	sched.clock = clock.NewFakeClock(time.Now())
	return sched
}

// makeFailedJobForRetry builds a job with one failed run, ready for the
// failure-handling code path to evaluate.
func makeFailedJobForRetry(t *testing.T, sched *Scheduler) *jobdb.Job {
	t.Helper()
	jobId := util.NewULID()
	job := testfixtures.NewJob(
		jobId,
		"testJobset",
		"testQueue",
		uint32(10),
		toInternalSchedulingInfo(schedulingInfo),
		false, // queued
		1,     // queuedVersion
		false, false, false,
		1,    // created
		true, // validated
	)
	failedRun := sched.jobDb.CreateRun(
		uuid.NewString(),
		0, // index
		jobId,
		1, // creationTime
		"testExecutor",
		"testNodeId",
		"testNode",
		"testPool",
		nil,
		false, false, false, false, nil,
		false, // preempted
		false, // succeeded
		true,  // failed
		false, // cancelled
		nil, nil, nil, nil, nil,
		false, // returned (not lease-returned)
		false, // runAttempted
	)
	return job.WithUpdatedRun(failedRun)
}

// makePreemptRequestedJobForRetry builds a leased, preemptible job whose run
// has PreemptRequested=true and is not yet marked failed, mirroring the state
// the scheduler sees the cycle after `armadactl preempt job` lands.
func makePreemptRequestedJobForRetry(t *testing.T, sched *Scheduler) *jobdb.Job {
	t.Helper()
	jobId := util.NewULID()
	job := testfixtures.NewJob(
		jobId,
		"testJobset",
		"testQueue",
		uint32(10),
		toInternalSchedulingInfo(preemptibleSchedulingInfo),
		false, // queued
		1,     // queuedVersion
		false, false, false,
		1,    // created
		true, // validated
	)
	run := sched.jobDb.CreateRun(
		uuid.NewString(),
		0, // index
		jobId,
		1, // creationTime
		"testExecutor",
		"testNodeId",
		"testNode",
		"testPool",
		nil,
		true,  // leased
		false, // pending
		true,  // running
		true,  // preemptRequested
		nil,
		false, // preempted
		false, // succeeded
		false, // failed
		false, // cancelled
		nil, nil, nil, nil, nil,
		false, // returned
		false, // runAttempted
	)
	return job.WithUpdatedRun(run)
}

func containerErrorWithExitCode(code int32) *armadaevents.Error {
	return &armadaevents.Error{
		Reason: &armadaevents.Error_PodError{
			PodError: &armadaevents.PodError{
				ContainerErrors: []*armadaevents.ContainerError{
					{
						ExitCode: code,
						Message:  "exit",
					},
				},
				KubernetesReason: armadaevents.KubernetesReason_AppError,
			},
		},
	}
}

func TestRetryPolicy_FFOff_NoEngineEvaluation(t *testing.T) {
	policy, err := retry.ConvertPolicy(&api.RetryPolicy{
		Name:          "test-policy",
		RetryLimit:    3,
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
		Rules: []*api.RetryRule{{
			Action:      api.RetryAction_RETRY_ACTION_RETRY,
			OnExitCodes: &api.RetryExitCodeMatcher{Operator: api.ExitCodeOperator_EXIT_CODE_OPERATOR_IN, Values: []int32{42}},
		}},
	})
	require.NoError(t, err)

	sched := makeRetryTestScheduler(t, false, fakePolicyCache{"test-policy": policy})
	job := makeFailedJobForRetry(t, sched)

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	jobErrors := map[string]*armadaevents.Error{job.LatestRun().Id(): containerErrorWithExitCode(42)}

	events, err := sched.generateUpdateMessagesFromJob(armadacontext.Background(), job, jobErrors, nil, txn)
	require.NoError(t, err)
	require.NotNil(t, events)

	hasRequeue, hasErrors := classifyEvents(events.Events)
	assert.False(t, hasRequeue, "FF off must not emit JobRequeued via engine")
	assert.True(t, hasErrors, "FF off must still emit terminal JobErrors")
}

func TestRetryPolicy_FFOn_RetryDecision(t *testing.T) {
	policy, err := retry.ConvertPolicy(&api.RetryPolicy{
		Name:          "test-policy",
		RetryLimit:    3,
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
		Rules: []*api.RetryRule{{
			Action:      api.RetryAction_RETRY_ACTION_RETRY,
			OnExitCodes: &api.RetryExitCodeMatcher{Operator: api.ExitCodeOperator_EXIT_CODE_OPERATOR_IN, Values: []int32{42}},
		}},
	})
	require.NoError(t, err)

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"test-policy": policy})
	job := makeFailedJobForRetry(t, sched)

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	jobErrors := map[string]*armadaevents.Error{job.LatestRun().Id(): containerErrorWithExitCode(42)}
	queueRetryPolicies := map[string]string{"testQueue": "test-policy"}

	events, err := sched.generateUpdateMessagesFromJob(armadacontext.Background(), job, jobErrors, queueRetryPolicies, txn)
	require.NoError(t, err)
	require.NotNil(t, events)

	hasRequeue, hasErrors := classifyEvents(events.Events)
	assert.True(t, hasRequeue, "FF on with matching retry rule must emit JobRequeued")
	assert.True(t, hasErrors, "FF on with retry decision must emit a non-terminal JobErrors so the api event stream surfaces the retry")
	assert.False(t, hasTerminalError(events.Events), "the emitted JobErrors must be non-terminal (Terminal=false) so it converts to JobFailedEvent{retryable=true}")

	// Simulate the next scheduling cycle creating a new run and assert the
	// run index advances. The scheduler-side WithNewRun derives index from
	// len(runsById), so the second run must be index 1.
	updatedJob := txn.GetById(job.Id())
	require.NotNil(t, updatedJob)
	relaunched := updatedJob.WithQueued(false).WithNewRun("testExecutor", "testNodeId", "testNode", "testPool", 0)
	assert.Equal(t, uint32(1), relaunched.LatestRun().Index(),
		"the executor pod-name suffix must change on retry: index 0 then index 1")
}

func TestRetryPolicy_FFOn_PolicyLimitCapsRetries(t *testing.T) {
	policy, err := retry.ConvertPolicy(&api.RetryPolicy{
		Name:          "test-policy",
		RetryLimit:    2,
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
		Rules: []*api.RetryRule{{
			Action:      api.RetryAction_RETRY_ACTION_RETRY,
			OnExitCodes: &api.RetryExitCodeMatcher{Operator: api.ExitCodeOperator_EXIT_CODE_OPERATOR_IN, Values: []int32{42}},
		}},
	})
	require.NoError(t, err)

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"test-policy": policy})

	// retryLimit=2 means 2 retries are allowed. With three failed runs already
	// (initial + 2 retries), the policy cap must trip on the next evaluation.
	jobId := util.NewULID()
	job := testfixtures.NewJob(jobId, "testJobset", "testQueue", uint32(10), toInternalSchedulingInfo(schedulingInfo), false, 1, false, false, false, 1, true)
	for i := uint32(0); i < 3; i++ {
		failedRun := sched.jobDb.CreateRun(uuid.NewString(), i, jobId, 1, "testExecutor", "testNodeId", "testNode", "testPool", nil,
			false, false, false, false, nil, false, false, true, false, nil, nil, nil, nil, nil, false, false)
		job = job.WithUpdatedRun(failedRun)
	}
	require.Equal(t, uint32(3), job.FailureCount())

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	jobErrors := map[string]*armadaevents.Error{job.LatestRun().Id(): containerErrorWithExitCode(42)}
	queueRetryPolicies := map[string]string{"testQueue": "test-policy"}

	events, err := sched.generateUpdateMessagesFromJob(armadacontext.Background(), job, jobErrors, queueRetryPolicies, txn)
	require.NoError(t, err)
	require.NotNil(t, events)

	hasRequeue, _ := classifyEvents(events.Events)
	assert.False(t, hasRequeue, "engine at retry limit must not emit JobRequeued")
	msg := terminalErrorMessage(events.Events)
	assert.Contains(t, msg, "Retry policy:",
		"terminal failure must surface the engine reason (operators rely on it for audit)")
	assert.Contains(t, msg, "policy retry limit",
		"reason must indicate the policy retry limit was hit")
}

func TestRetryPolicy_FFOn_TerminalFailPreservesFailureCategory(t *testing.T) {
	policy, err := retry.ConvertPolicy(&api.RetryPolicy{
		Name:          "test-policy",
		RetryLimit:    0, // unlimited at policy level
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
		Rules: []*api.RetryRule{{
			Action:      api.RetryAction_RETRY_ACTION_FAIL,
			OnExitCodes: &api.RetryExitCodeMatcher{Operator: api.ExitCodeOperator_EXIT_CODE_OPERATOR_IN, Values: []int32{42}},
		}},
	})
	require.NoError(t, err)

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"test-policy": policy})
	job := makeFailedJobForRetry(t, sched)

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	runError := containerErrorWithExitCode(42)
	runError.FailureCategory = "ApplicationError"
	runError.FailureSubcategory = "ExitCode42"
	jobErrors := map[string]*armadaevents.Error{job.LatestRun().Id(): runError}
	queueRetryPolicies := map[string]string{"testQueue": "test-policy"}

	events, err := sched.generateUpdateMessagesFromJob(armadacontext.Background(), job, jobErrors, queueRetryPolicies, txn)
	require.NoError(t, err)
	require.NotNil(t, events)

	for _, e := range events.Events {
		if je := e.GetJobErrors(); je != nil {
			for _, errEv := range je.Errors {
				if !errEv.Terminal {
					continue
				}
				assert.Equal(t, "ApplicationError", errEv.FailureCategory,
					"policy-fail terminal error must carry the original FailureCategory")
				assert.Equal(t, "ExitCode42", errEv.FailureSubcategory,
					"policy-fail terminal error must carry the original FailureSubcategory")
				return
			}
		}
	}
	t.Fatal("expected a terminal JobErrors event")
}

func TestRetryPolicy_FFOn_MissingPolicyFallsThrough(t *testing.T) {
	sched := makeRetryTestScheduler(t, true, fakePolicyCache{}) // empty cache

	job := makeFailedJobForRetry(t, sched)
	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	jobErrors := map[string]*armadaevents.Error{job.LatestRun().Id(): containerErrorWithExitCode(42)}
	queueRetryPolicies := map[string]string{"testQueue": "test-policy"}

	events, err := sched.generateUpdateMessagesFromJob(armadacontext.Background(), job, jobErrors, queueRetryPolicies, txn)
	require.NoError(t, err)
	require.NotNil(t, events)

	_, hasErrors := classifyEvents(events.Events)
	assert.True(t, hasErrors, "missing policy must not crash; falls back to legacy terminal-failure path")
}

func classifyEvents(events []*armadaevents.EventSequence_Event) (hasRequeue bool, hasErrors bool) {
	for _, e := range events {
		if e.GetJobRequeued() != nil {
			hasRequeue = true
		}
		if e.GetJobErrors() != nil {
			hasErrors = true
		}
	}
	return
}

// hasTerminalError reports whether any emitted JobErrors carries a terminal
// Error - the engine retry path must emit only non-terminal errors so the
// api conversion stamps retryable=true on the resulting JobFailedEvent.
func hasTerminalError(events []*armadaevents.EventSequence_Event) bool {
	for _, e := range events {
		je := e.GetJobErrors()
		if je == nil {
			continue
		}
		for _, errEv := range je.Errors {
			if errEv.Terminal {
				return true
			}
		}
	}
	return false
}

func terminalErrorMessage(events []*armadaevents.EventSequence_Event) string {
	for _, e := range events {
		if je := e.GetJobErrors(); je != nil {
			for _, errEv := range je.Errors {
				if mre := errEv.GetMaxRunsExceeded(); mre != nil {
					return mre.Message
				}
			}
		}
	}
	return ""
}

// TestRetryPolicy_FFOn_GangJobSkipped pins gang skip; see
// notes/retry-policy/gang-retry.md for why gangs are out of scope.
func TestRetryPolicy_FFOn_GangJobSkipped(t *testing.T) {
	policy, err := retry.ConvertPolicy(&api.RetryPolicy{
		Name:          "test-policy",
		RetryLimit:    3,
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
		Rules: []*api.RetryRule{{
			Action:      api.RetryAction_RETRY_ACTION_RETRY,
			OnExitCodes: &api.RetryExitCodeMatcher{Operator: api.ExitCodeOperator_EXIT_CODE_OPERATOR_IN, Values: []int32{42}},
		}},
	})
	require.NoError(t, err)

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"test-policy": policy})
	job := makeFailedJobForRetry(t, sched).WithGangInfo(jobdb.CreateGangInfo("gang-1", 3, ""))
	require.True(t, job.IsInGang(), "test fixture must produce a gang job")

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	jobErrors := map[string]*armadaevents.Error{job.LatestRun().Id(): containerErrorWithExitCode(42)}
	queueRetryPolicies := map[string]string{"testQueue": "test-policy"}

	events, err := sched.generateUpdateMessagesFromJob(armadacontext.Background(), job, jobErrors, queueRetryPolicies, txn)
	require.NoError(t, err)
	require.NotNil(t, events)

	hasRequeue, hasErrors := classifyEvents(events.Events)
	assert.False(t, hasRequeue, "gang job must NOT be requeued by the engine")
	assert.True(t, hasErrors, "gang job must reach terminal failure via legacy path")
}

// TestRetryPolicy_FFOn_PreemptedRunsDoNotCountAgainstGlobalCap guards against
// the scheduler-algo's fresh-run-per-preemption shape (preempted=true,
// failed=false) burning global-cap budget before any policy retry happened.
func TestRetryPolicy_FFOn_PreemptedRunsDoNotCountAgainstGlobalCap(t *testing.T) {
	policy, err := retry.ConvertPolicy(&api.RetryPolicy{
		Name:          "test-policy",
		RetryLimit:    0, // unlimited at policy level; global cap is the only gate
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
		Rules: []*api.RetryRule{{
			Action:      api.RetryAction_RETRY_ACTION_RETRY,
			OnExitCodes: &api.RetryExitCodeMatcher{Operator: api.ExitCodeOperator_EXIT_CODE_OPERATOR_IN, Values: []int32{42}},
		}},
	})
	require.NoError(t, err)

	// Global cap of 2: with the bug, three preempted-but-not-failed runs plus
	// one failed run (totalRuns=4) would trip the cap. With the fix
	// (totalRuns counts only failed=true), failureCount=1 < 2 retries.
	sched := makeRetryTestSchedulerWithGlobalMax(t, true, fakePolicyCache{"test-policy": policy}, 2)

	jobId := util.NewULID()
	job := testfixtures.NewJob(
		jobId, "testJobset", "testQueue", uint32(10),
		toInternalSchedulingInfo(schedulingInfo),
		false, 1, false, false, false, 1, true,
	)
	// Add three preempted-but-not-failed runs - the scheduler-algo preemption
	// shape that creates new runs without burning policy-retry budget.
	for i := 0; i < 3; i++ {
		preemptedRun := sched.jobDb.CreateRun(
			uuid.NewString(), uint32(i), jobId, 1,
			"testExecutor", "testNodeId", "testNode", "testPool",
			nil, false, false, false, false, nil,
			true,  // preempted
			false, // succeeded
			false, // failed
			false, // cancelled
			nil, nil, nil, nil, nil,
			false, false,
		)
		job = job.WithUpdatedRun(preemptedRun)
	}
	// Add a single failed run that the engine will evaluate.
	failedRun := sched.jobDb.CreateRun(
		uuid.NewString(), 3, jobId, 1,
		"testExecutor", "testNodeId", "testNode", "testPool",
		nil, false, false, false, false, nil,
		false, // preempted
		false, // succeeded
		true,  // failed
		false, // cancelled
		nil, nil, nil, nil, nil,
		false, false,
	)
	job = job.WithUpdatedRun(failedRun)
	require.Equal(t, uint32(1), job.FailureCount(), "fixture must have exactly one failed run")
	require.Equal(t, 4, len(job.AllRuns()), "fixture must have four total runs")

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	jobErrors := map[string]*armadaevents.Error{job.LatestRun().Id(): containerErrorWithExitCode(42)}
	queueRetryPolicies := map[string]string{"testQueue": "test-policy"}

	events, err := sched.generateUpdateMessagesFromJob(armadacontext.Background(), job, jobErrors, queueRetryPolicies, txn)
	require.NoError(t, err)
	require.NotNil(t, events)

	hasRequeue, _ := classifyEvents(events.Events)
	assert.True(t, hasRequeue,
		"global cap must measure failures, not preempted runs; with 1 failure under a cap of 2 the engine must still retry")
	assert.False(t, hasTerminalError(events.Events), "engine retry path must not emit a terminal JobErrors")
}

func TestRetryPolicy_FFOn_UserPreemptRequeuesWhenEngineMatches(t *testing.T) {
	policy, err := retry.ConvertPolicy(&api.RetryPolicy{
		Name:          "preempt-policy",
		RetryLimit:    3,
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
		Rules: []*api.RetryRule{{
			Action:       api.RetryAction_RETRY_ACTION_RETRY,
			OnConditions: []string{"Preempted"},
		}},
	})
	require.NoError(t, err)

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"preempt-policy": policy})
	job := makePreemptRequestedJobForRetry(t, sched)

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	queueRetryPolicies := map[string]string{"testQueue": "preempt-policy"}

	events, err := sched.generateUpdateMessagesFromJob(armadacontext.Background(), job, nil, queueRetryPolicies, txn)
	require.NoError(t, err)
	require.NotNil(t, events)

	hasRequeue, hasErrors := classifyEvents(events.Events)
	assert.True(t, hasRequeue, "engine must emit JobRequeued for matched preemption")
	assert.True(t, hasErrors, "engine must emit a non-terminal JobErrors so the api stream sees retryable=true")
	assert.False(t, hasTerminalError(events.Events), "engine retry path must not emit a terminal JobErrors on preemption")

	updated := txn.GetById(job.Id())
	require.NotNil(t, updated)
	assert.True(t, updated.Queued(), "job must be queued for retry after engine match")
	assert.False(t, updated.Failed(), "job must not be marked failed when engine retries")
	assert.True(t, updated.LatestRun().Failed(),
		"run must be marked failed so the `!Failed()` guard on the user-preempt branch stops it firing every cycle; Failed survives jobdb reconciliation while Preempted does not")
}

func TestRetryPolicy_FFOn_UserPreemptTerminalWhenNoMatch(t *testing.T) {
	policy, err := retry.ConvertPolicy(&api.RetryPolicy{
		Name:          "no-preempt-policy",
		RetryLimit:    3,
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
		Rules: []*api.RetryRule{{
			Action:      api.RetryAction_RETRY_ACTION_RETRY,
			OnExitCodes: &api.RetryExitCodeMatcher{Operator: api.ExitCodeOperator_EXIT_CODE_OPERATOR_IN, Values: []int32{42}},
		}},
	})
	require.NoError(t, err)

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"no-preempt-policy": policy})
	job := makePreemptRequestedJobForRetry(t, sched)

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	queueRetryPolicies := map[string]string{"testQueue": "no-preempt-policy"}

	events, err := sched.generateUpdateMessagesFromJob(armadacontext.Background(), job, nil, queueRetryPolicies, txn)
	require.NoError(t, err)
	require.NotNil(t, events)

	hasRequeue, hasErrors := classifyEvents(events.Events)
	assert.False(t, hasRequeue, "no matching rule must not emit JobRequeued")
	assert.True(t, hasErrors, "no matching rule must emit terminal JobErrors")
	assert.True(t, hasTerminalError(events.Events), "non-match preemption must terminally fail the job")

	updated := txn.GetById(job.Id())
	require.NotNil(t, updated)
	assert.True(t, updated.Failed(), "job must be terminally failed when engine does not match")
}

// TestRetryPolicy_FFOn_UserPreemptCountsCurrentRunAgainstLimit pins that the
// retry limit applies to the run being evaluated, not just to prior runs.
// The branch fires while lastRun.failed=false, so we must mark the run failed
// before consulting the engine. Without that, FailureCount() returns N-1
// instead of N and retryLimit is off-by-one.
func TestRetryPolicy_FFOn_UserPreemptCountsCurrentRunAgainstLimit(t *testing.T) {
	policy, err := retry.ConvertPolicy(&api.RetryPolicy{
		Name:          "preempt-policy",
		RetryLimit:    1, // 1 retry allowed; the second preemption must be terminal
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
		Rules: []*api.RetryRule{{
			Action:       api.RetryAction_RETRY_ACTION_RETRY,
			OnConditions: []string{"Preempted"},
		}},
	})
	require.NoError(t, err)

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"preempt-policy": policy})

	// Job has already exhausted its 1 retry: one prior failed run plus a
	// fresh PreemptRequested run that has not yet been marked failed.
	jobId := util.NewULID()
	job := testfixtures.NewJob(
		jobId, "testJobset", "testQueue", uint32(10),
		toInternalSchedulingInfo(preemptibleSchedulingInfo),
		false, 1, false, false, false, 1, true,
	)
	priorRun := sched.jobDb.CreateRun(
		uuid.NewString(), 0, jobId, 1,
		"testExecutor", "testNodeId", "testNode", "testPool", nil,
		false, false, false, false, nil,
		false, false, true, false, // failed=true
		nil, nil, nil, nil, nil,
		false, false,
	)
	currentRun := sched.jobDb.CreateRun(
		uuid.NewString(), 1, jobId, 1,
		"testExecutor", "testNodeId", "testNode", "testPool", nil,
		true, false, true, true, nil, // running + preemptRequested
		false, false, false, false, // failed=false (matches the branch guard)
		nil, nil, nil, nil, nil,
		false, false,
	)
	job = job.WithUpdatedRun(priorRun).WithUpdatedRun(currentRun)
	require.Equal(t, uint32(1), job.FailureCount(),
		"only the prior run is failed at branch entry; FailureCount excludes the current run")

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	queueRetryPolicies := map[string]string{"testQueue": "preempt-policy"}

	events, err := sched.generateUpdateMessagesFromJob(armadacontext.Background(), job, nil, queueRetryPolicies, txn)
	require.NoError(t, err)
	require.NotNil(t, events)

	hasRequeue, _ := classifyEvents(events.Events)
	assert.False(t, hasRequeue,
		"retryLimit=1 plus one prior failed run plus the current run must terminate; "+
			"without the WithFailed(true) pre-evaluation step the engine sees FailureCount=1 instead of 2 and lets a second retry through")
	assert.True(t, hasTerminalError(events.Events), "engine cap must emit a terminal JobErrors")
}

func TestRetryPolicy_FFOn_AlgoPreemptRequeuesWhenEngineMatches(t *testing.T) {
	policy, err := retry.ConvertPolicy(&api.RetryPolicy{
		Name:          "preempt-policy",
		RetryLimit:    3,
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
		Rules: []*api.RetryRule{{
			Action:       api.RetryAction_RETRY_ACTION_RETRY,
			OnConditions: []string{"Preempted"},
		}},
	})
	require.NoError(t, err)

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"preempt-policy": policy})

	// Build a job in the algo-preempted state: queued=false, failed=true (job),
	// run.failed=true (algo set both in-cycle at scheduling_algo.go:802/806).
	jobId := util.NewULID()
	job := testfixtures.NewJob(
		jobId, "testJobset", "testQueue", uint32(10),
		toInternalSchedulingInfo(preemptibleSchedulingInfo),
		false, // queued
		1,     // queuedVersion
		false, false, false,
		1, true,
	)
	run := sched.jobDb.CreateRun(
		uuid.NewString(), 0, jobId, 1,
		"testExecutor", "testNodeId", "testNode", "testPool", nil,
		true,  // leased
		false, // pending
		true,  // running
		false, // preemptRequested
		nil,
		false, // preempted (algo doesn't set this in-memory)
		false, // succeeded
		true,  // failed (algo set)
		false, // cancelled
		nil, nil, nil, nil, nil,
		false, false,
	)
	job = job.WithUpdatedRun(run).WithFailed(true)

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	jctx := schedulercontext.JobSchedulingContextsFromJobs([]*jobdb.Job{job})[0]
	jctx.PreemptionDescription = "preempted by higher priority gang"
	result := &scheduling.SchedulerResult{
		PoolResults: []*scheduling.PoolSchedulingResult{
			{SchedulingResult: &scheduling.SchedulingResult{PreemptedJobs: []*schedulercontext.JobSchedulingContext{jctx}}},
		},
	}
	queueRetryPolicies := map[string]string{"testQueue": "preempt-policy"}

	retried, err := sched.applyRetryPolicyToAlgoPreemptions(armadacontext.Background(), txn, result, queueRetryPolicies)
	require.NoError(t, err)
	require.True(t, retried[jobId], "engine match must mark job for retry override")

	updated := txn.GetById(jobId)
	require.NotNil(t, updated)
	assert.True(t, updated.Queued(), "job must be re-queued after engine override")
	assert.False(t, updated.Failed(), "job must clear failed=true after engine override")
	assert.Equal(t, int32(2), updated.QueuedVersion(), "QueuedVersion must increment so JobRequeued carries the right version")
	assert.True(t, updated.LatestRun().Failed(),
		"run must keep failed=true; Failed survives jobdb reconciliation and acts as the dedupe signal")
	assert.Equal(t, jobId, jctx.Job.Id(), "jctx.Job must point at the updated job so the events helper reads the new QueuedVersion")
}

func TestRetryPolicy_FFOn_AlgoPreemptTerminalWhenNoMatch(t *testing.T) {
	policy, err := retry.ConvertPolicy(&api.RetryPolicy{
		Name:          "no-preempt-policy",
		RetryLimit:    3,
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
		Rules: []*api.RetryRule{{
			Action:      api.RetryAction_RETRY_ACTION_RETRY,
			OnExitCodes: &api.RetryExitCodeMatcher{Operator: api.ExitCodeOperator_EXIT_CODE_OPERATOR_IN, Values: []int32{42}},
		}},
	})
	require.NoError(t, err)

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"no-preempt-policy": policy})

	jobId := util.NewULID()
	job := testfixtures.NewJob(
		jobId, "testJobset", "testQueue", uint32(10),
		toInternalSchedulingInfo(preemptibleSchedulingInfo),
		false, 1, false, false, false, 1, true,
	)
	run := sched.jobDb.CreateRun(
		uuid.NewString(), 0, jobId, 1,
		"testExecutor", "testNodeId", "testNode", "testPool", nil,
		true, false, true, false, nil,
		false, false, true, false,
		nil, nil, nil, nil, nil,
		false, false,
	)
	job = job.WithUpdatedRun(run).WithFailed(true)

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	jctx := schedulercontext.JobSchedulingContextsFromJobs([]*jobdb.Job{job})[0]
	jctx.PreemptionDescription = "preempted by higher priority gang"
	result := &scheduling.SchedulerResult{
		PoolResults: []*scheduling.PoolSchedulingResult{
			{SchedulingResult: &scheduling.SchedulingResult{PreemptedJobs: []*schedulercontext.JobSchedulingContext{jctx}}},
		},
	}
	queueRetryPolicies := map[string]string{"testQueue": "no-preempt-policy"}

	retried, err := sched.applyRetryPolicyToAlgoPreemptions(armadacontext.Background(), txn, result, queueRetryPolicies)
	require.NoError(t, err)
	require.Empty(t, retried, "no matching rule must leave the algo's terminal-fail untouched")

	updated := txn.GetById(jobId)
	require.NotNil(t, updated)
	assert.True(t, updated.Failed(), "job must keep failed=true when engine does not match")
}
