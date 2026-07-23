package scheduler

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	clock "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/errormatch"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/leaderelection"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/pricing"
	"github.com/armadaproject/armada/internal/scheduler/retry"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/runner"
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
	// GlobalMaxRetries 0 is the kill switch, so default to a cap high enough
	// that per-policy limits are the only gate in most tests.
	return makeRetryTestSchedulerWithGlobalMax(t, ffEnabled, policyCache, 10)
}

// makeRetryTestSchedulerWithGlobalMax is the same as makeRetryTestScheduler
// but lets the caller set the global retry cap, used to pin the contract
// that the cap counts failed runs only.
func makeRetryTestSchedulerWithGlobalMax(t *testing.T, ffEnabled bool, policyCache retry.PolicyCache, globalMaxRetries uint) *Scheduler {
	t.Helper()
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	queueCache := &testQueueCache{queues: []*api.Queue{
		{Name: "testQueue", RetryPolicies: []string{"test-policy"}},
	}}

	sched, err := NewScheduler(
		jobDb,
		&testJobRepository{},
		&testExecutorRepository{},
		runner.NewSyncSchedulingRunner(&testSchedulingAlgo{}),
		leaderelection.NewStandaloneLeaderController(),
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

// mkPolicy converts an api.RetryPolicy named "test-policy" into the engine
// Policy the fakePolicyCache serves, failing the test on a conversion error.
func mkPolicy(t *testing.T, limit uint32, defaultAction api.RetryAction, rules ...*api.RetryRule) *retry.Policy {
	t.Helper()
	policy, err := retry.ConvertPolicy(&api.RetryPolicy{
		Name:          "test-policy",
		RetryLimit:    limit,
		DefaultAction: defaultAction,
		Rules:         rules,
	})
	require.NoError(t, err)
	return policy
}

// jobRunOpts describes the run history to stamp onto a retry-test job: zero or
// more preempted runs, zero or more failed runs, and an optional active
// leased/running run added last so it becomes the job's LatestRun.
type jobRunOpts struct {
	schedulingInfo *schedulerobjects.JobSchedulingInfo
	leased         bool
	running        bool
	failedRuns     int
	preemptedRuns  int
	returned       bool
	runAttempted   bool
}

// makeRetryJob builds a job whose run history matches opts, centralizing the
// error-prone 24-positional-arg CreateRun call the retry tests share.
func makeRetryJob(t *testing.T, sched *Scheduler, opts jobRunOpts) *jobdb.Job {
	t.Helper()
	jobId := util.NewULID()
	job := testfixtures.NewJob(
		jobId, "testJobset", "testQueue", uint32(10),
		toInternalSchedulingInfo(opts.schedulingInfo),
		false, 1, false, false, false, 1, true,
	)
	terminalRun := func(preempted, failed bool) *jobdb.JobRun {
		return sched.jobDb.CreateRun(
			uuid.NewString(), jobId, 1, "testExecutor", "testNodeId", "testNode", "testPool", nil,
			false, false, false, false, nil,
			preempted, false, failed, false,
			nil, nil, nil, nil, nil,
			opts.returned, opts.runAttempted,
		)
	}
	for i := 0; i < opts.preemptedRuns; i++ {
		job = job.WithUpdatedRun(terminalRun(true, false))
	}
	for i := 0; i < opts.failedRuns; i++ {
		job = job.WithUpdatedRun(terminalRun(false, true))
	}
	if opts.running {
		run := sched.jobDb.CreateRun(
			uuid.NewString(), jobId, 1, "testExecutor", "testNodeId", "testNode", "testPool", nil,
			opts.leased, false, true, false, nil,
			false, false, false, false,
			nil, nil, nil, nil, nil,
			opts.returned, opts.runAttempted,
		)
		job = job.WithUpdatedRun(run)
	}
	return job
}

// runFailurePath drives generateUpdateMessagesFromJob for a single job through
// the standard {"testQueue":"test-policy"} mapping and returns the emitted
// events plus the open write txn (the caller must defer txn.Abort()).
func runFailurePath(t *testing.T, sched *Scheduler, job *jobdb.Job, runErr *armadaevents.Error) (*armadaevents.EventSequence, *jobdb.Txn) {
	t.Helper()
	txn := sched.jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))
	jobErrors := map[string]*armadaevents.Error{job.LatestRun().Id(): runErr}
	queueRetryPolicies := map[string]string{"testQueue": "test-policy"}
	events, err := sched.generateUpdateMessagesFromJob(armadacontext.Background(), job, jobErrors, queueRetryPolicies, txn)
	require.NoError(t, err)
	require.NotNil(t, events)
	return events, txn
}

// makeFailedJobForRetry builds a job with one failed run, ready for the
// failure-handling code path to evaluate.
func makeFailedJobForRetry(t *testing.T, sched *Scheduler) *jobdb.Job {
	t.Helper()
	return makeRetryJob(t, sched, jobRunOpts{schedulingInfo: schedulingInfo, failedRuns: 1})
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

// categorizedError returns a container run error tagged with the given failure
// category, so tests can drive the category-only retry engine.
func categorizedError(category string) *armadaevents.Error {
	err := containerErrorWithExitCode(42)
	err.FailureCategory = category
	return err
}

func TestRetryPolicy_FFOn_DefaultPolicyNameFallback(t *testing.T) {
	policy, err := retry.ConvertPolicy(&api.RetryPolicy{
		Name:          "default-policy",
		RetryLimit:    3,
		DefaultAction: api.RetryAction_RETRY_ACTION_RETRY,
	})
	require.NoError(t, err)

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"default-policy": policy})
	sched.retryPolicyConfig.DefaultPolicyName = "default-policy"

	job := makeFailedJobForRetry(t, sched)

	// An empty queue map means the queue has no attached policy, so resolution
	// must fall back to the default.
	shouldRetry, _, policyName, decided := sched.evaluateRetryPolicy(
		armadacontext.Background(), job, categorizedError("app-error"), map[string]string{})
	assert.True(t, decided, "default policy must produce a decision for an unattached queue")
	assert.Equal(t, "default-policy", policyName, "the decision must be attributed to the default policy")
	assert.True(t, shouldRetry, "default policy's Retry default action must apply")
}

func TestRetryPolicy_FFOn_UnattachedQueueWithoutDefaultIsUndecided(t *testing.T) {
	sched := makeRetryTestScheduler(t, true, fakePolicyCache{})
	job := makeFailedJobForRetry(t, sched)

	// No attached queue policy and no configured default means the name resolves
	// to empty, so the engine must leave the decision to the legacy path.
	_, _, _, decided := sched.evaluateRetryPolicy(
		armadacontext.Background(), job, categorizedError("app-error"), map[string]string{})
	assert.False(t, decided, "an unattached queue with no default policy must not be decided by the engine")
}

func TestRetryPolicy_FFOn_NilRunErrorIsUndecided(t *testing.T) {
	policy := mkPolicy(t, 3, api.RetryAction_RETRY_ACTION_RETRY, &api.RetryRule{
		Action:     api.RetryAction_RETRY_ACTION_RETRY,
		OnCategory: "app-error",
	})
	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"test-policy": policy})
	job := makeFailedJobForRetry(t, sched)

	// A nil runError is the ingester-race branch: the engine has nothing to match
	// on and must defer rather than fabricate a retry from a resolvable policy.
	_, _, _, decided := sched.evaluateRetryPolicy(
		armadacontext.Background(), job, nil, map[string]string{"testQueue": "test-policy"})
	assert.False(t, decided, "a nil runError must leave the decision to the legacy path even with a resolvable policy")
}

// Lease returns carry no failure category, so they match no rule and fall
// through to the legacy retry path rather than being terminally failed by a
// Fail default.
func TestRetryPolicy_FFOn_LeaseReturnDefersToLegacy(t *testing.T) {
	policy := mkPolicy(t, 3, api.RetryAction_RETRY_ACTION_FAIL, &api.RetryRule{
		Action:     api.RetryAction_RETRY_ACTION_RETRY,
		OnCategory: "app-error",
	})

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"test-policy": policy})
	job := makeFailedJobForRetry(t, sched)

	leaseReturned := &armadaevents.Error{
		Reason: &armadaevents.Error_PodLeaseReturned{
			PodLeaseReturned: &armadaevents.PodLeaseReturned{Message: "node drained"},
		},
	}
	shouldRetry, reason, _, decided := sched.evaluateRetryPolicy(
		armadacontext.Background(), job, leaseReturned, map[string]string{"testQueue": "test-policy"})
	assert.False(t, decided, "lease returns must not be decided by the engine, even under a Fail-default policy")
	assert.False(t, shouldRetry, "undecided verdict must not signal retry")
	assert.Empty(t, reason, "undecided verdict must not carry a reason")
}

func TestRetryPolicy_FFOn_RetryDecision(t *testing.T) {
	policy := mkPolicy(t, 3, api.RetryAction_RETRY_ACTION_FAIL, &api.RetryRule{
		Action:     api.RetryAction_RETRY_ACTION_RETRY,
		OnCategory: "app-error",
	})

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"test-policy": policy})
	job := makeFailedJobForRetry(t, sched)

	events, txn := runFailurePath(t, sched, job, categorizedError("app-error"))
	defer txn.Abort()

	hasRequeue, hasErrors := classifyEvents(events.Events)
	assert.True(t, hasRequeue, "FF on with matching retry rule must emit JobRequeued")
	assert.True(t, hasErrors, "FF on with retry decision must emit a non-terminal JobErrors so the api event stream surfaces the retry")
	assert.False(t, hasTerminalError(events.Events), "the emitted JobErrors must be non-terminal (Terminal=false) so it converts to JobFailedEvent{retryable=true}")
	if retryErr := nonTerminalError(events.Events); assert.NotNil(t, retryErr) {
		assert.Equal(t, "test-policy", retryErr.RetryPolicyName, "the retry event must record the deciding policy")
	}
}

func TestRetryPolicy_FFOn_PolicyLimitCapsRetries(t *testing.T) {
	policy := mkPolicy(t, 2, api.RetryAction_RETRY_ACTION_FAIL, &api.RetryRule{
		Action:     api.RetryAction_RETRY_ACTION_RETRY,
		OnCategory: "app-error",
	})

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"test-policy": policy})

	// The policy allows two retries. This job has already failed three times
	// (the initial attempt plus two retries), so the next failure must fail it.
	job := makeRetryJob(t, sched, jobRunOpts{schedulingInfo: schedulingInfo, failedRuns: 3})
	require.Equal(t, uint32(3), job.FailureCount())

	events, txn := runFailurePath(t, sched, job, categorizedError("app-error"))
	defer txn.Abort()

	hasRequeue, _ := classifyEvents(events.Events)
	assert.False(t, hasRequeue, "engine at retry limit must not emit JobRequeued")
	assert.Contains(t, terminalErrorMessage(events.Events), "Retry policy:",
		"terminal failure must surface the engine reason (operators rely on it for audit)")
}

// A policy Fail decision on a categorized PodError must terminally fail the job
// while preserving both the failure taxonomy (category + subcategory) and the
// original pod error detail (message + exit code) inside the policy verdict.
func TestRetryPolicy_FFOn_TerminalFailPreservesOriginalError(t *testing.T) {
	policy := mkPolicy(t, 0, api.RetryAction_RETRY_ACTION_FAIL, &api.RetryRule{
		Action:     api.RetryAction_RETRY_ACTION_FAIL,
		OnCategory: "user-error",
	})

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"test-policy": policy})
	job := makeFailedJobForRetry(t, sched)

	runError := &armadaevents.Error{
		FailureCategory:    "user-error",
		FailureSubcategory: "OutOfMemory",
		Reason: &armadaevents.Error_PodError{
			PodError: &armadaevents.PodError{
				Message:         "OOMKilled",
				ContainerErrors: []*armadaevents.ContainerError{{ExitCode: 137, Message: "out of memory"}},
			},
		},
	}
	events, txn := runFailurePath(t, sched, job, runError)
	defer txn.Abort()

	terminal := terminalError(events.Events)
	require.NotNil(t, terminal, "policy Fail must emit a terminal JobErrors event")
	assert.Equal(t, "user-error", terminal.FailureCategory,
		"policy-fail terminal error must carry the original FailureCategory")
	assert.Equal(t, "OutOfMemory", terminal.FailureSubcategory,
		"policy-fail terminal error must carry the original FailureSubcategory")

	mre := terminal.GetMaxRunsExceeded()
	require.NotNil(t, mre, "the terminal error must be a MaxRunsExceeded policy verdict")
	assert.Contains(t, mre.Message, "Retry policy:", "must surface the policy verdict")
	assert.Contains(t, mre.Message, "OOMKilled", "must preserve the original pod error message")
	assert.Contains(t, mre.Message, "137", "must preserve the original container exit code")
}

func TestRetryPolicy_FFOn_MissingPolicyFallsThrough(t *testing.T) {
	sched := makeRetryTestScheduler(t, true, fakePolicyCache{}) // empty cache

	job := makeFailedJobForRetry(t, sched)
	events, txn := runFailurePath(t, sched, job, categorizedError("app-error"))
	defer txn.Abort()

	hasRequeue, hasErrors := classifyEvents(events.Events)
	assert.True(t, hasErrors, "missing policy must not crash; falls back to legacy terminal-failure path")
	assert.False(t, hasRequeue, "missing policy must not requeue; the legacy path terminally fails the job")
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
// Error. The engine retry path must emit only non-terminal errors so the
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

// terminalError returns the terminal Error carried by an emitted JobErrors
// event, or nil if none is terminal.
func terminalError(events []*armadaevents.EventSequence_Event) *armadaevents.Error {
	for _, e := range events {
		if je := e.GetJobErrors(); je != nil {
			for _, errEv := range je.Errors {
				if errEv.Terminal {
					return errEv
				}
			}
		}
	}
	return nil
}

func nonTerminalError(events []*armadaevents.EventSequence_Event) *armadaevents.Error {
	for _, e := range events {
		if je := e.GetJobErrors(); je != nil {
			for _, errEv := range je.Errors {
				if !errEv.Terminal {
					return errEv
				}
			}
		}
	}
	return nil
}

func terminalErrorMessage(events []*armadaevents.EventSequence_Event) string {
	if te := terminalError(events); te != nil {
		if mre := te.GetMaxRunsExceeded(); mre != nil {
			return mre.Message
		}
	}
	return ""
}

func TestRetryPolicy_FFOn_GlobalCapExcludesPreemptions(t *testing.T) {
	// RetryLimit is set high so the per-policy limit is never the gate; this
	// test isolates the global cap.
	policy := mkPolicy(t, 10, api.RetryAction_RETRY_ACTION_FAIL, &api.RetryRule{
		Action:     api.RetryAction_RETRY_ACTION_RETRY,
		OnCategory: "app-error",
	})

	// Global cap of 2, three preempted runs plus one failed run. Preemptions
	// are the scheduler's own action and must not consume the global budget:
	// genuineRuns = 4 - 3 = 1, so the failed run is the job's first genuine
	// attempt and must still retry.
	sched := makeRetryTestSchedulerWithGlobalMax(t, true, fakePolicyCache{"test-policy": policy}, 2)
	job := makeRetryJob(t, sched, jobRunOpts{schedulingInfo: schedulingInfo, preemptedRuns: 3, failedRuns: 1})
	require.Equal(t, uint32(1), job.FailureCount(), "fixture must have exactly one failed run")
	require.Equal(t, 4, len(job.AllRuns()), "fixture must have four total runs")

	events, txn := runFailurePath(t, sched, job, categorizedError("app-error"))
	defer txn.Abort()

	hasRequeue, _ := classifyEvents(events.Events)
	assert.True(t, hasRequeue,
		"preemptions must not consume the global cap: 3 preemptions + 1 failure with a cap of 2 must still retry")
}

func TestRetryPolicy_FFOn_GlobalMaxZeroDisablesRetries(t *testing.T) {
	policy := mkPolicy(t, 3, api.RetryAction_RETRY_ACTION_FAIL, &api.RetryRule{
		Action:     api.RetryAction_RETRY_ACTION_RETRY,
		OnCategory: "app-error",
	})

	sched := makeRetryTestSchedulerWithGlobalMax(t, true, fakePolicyCache{"test-policy": policy}, 0)
	job := makeFailedJobForRetry(t, sched)

	events, txn := runFailurePath(t, sched, job, categorizedError("app-error"))
	defer txn.Abort()

	hasRequeue, _ := classifyEvents(events.Events)
	assert.False(t, hasRequeue, "GlobalMaxRetries 0 must never retry")
	updated := txn.GetById(job.Id())
	require.NotNil(t, updated)
	assert.True(t, updated.Failed(), "GlobalMaxRetries 0 kill switch must terminally fail the job")
}

// makeRunningJobOnExecutor builds a leased, running, non-gang job on
// testExecutor, the state the lease-expiry sweep sees for jobs on an executor
// that stopped heartbeating.
func makeRunningJobOnExecutor(t *testing.T, sched *Scheduler) *jobdb.Job {
	t.Helper()
	return makeRetryJob(t, sched, jobRunOpts{schedulingInfo: schedulingInfo, leased: true, running: true})
}

func TestRetryPolicy_FFOn_LeaseExpiryRetriesWhenPolicyMatches(t *testing.T) {
	policy := mkPolicy(t, 3, api.RetryAction_RETRY_ACTION_FAIL, &api.RetryRule{
		Action:        api.RetryAction_RETRY_ACTION_RETRY,
		OnCategory:    errormatch.CategoryInternal,
		OnSubcategory: errormatch.SubcategoryLeaseExpired,
	})

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"test-policy": policy})
	// testExecutor stopped heartbeating well past the 1h executorTimeout.
	sched.executorRepository = &testExecutorRepository{
		updateTimes: map[string]time.Time{"testExecutor": sched.clock.Now().Add(-2 * time.Hour)},
	}
	job := makeRunningJobOnExecutor(t, sched)

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	eventSequences, err := sched.expireJobsIfNecessary(armadacontext.Background(), txn)
	require.NoError(t, err)
	require.Len(t, eventSequences, 1)

	evs := eventSequences[0].Events
	require.Len(t, evs, 3, "lease-expiry retry must emit terminal JobRunErrors + non-terminal JobErrors + JobRequeued")

	// The first event must be the run-scoped JobRunErrors: it marks the run
	// terminated in the DB so a returning executor cancels it. The JobErrors and
	// JobRequeued that follow are order-independent, so match them by type.
	jre := evs[0].GetJobRunErrors()
	require.NotNil(t, jre, "first event must be the terminal run-scoped JobRunErrors")
	require.Len(t, jre.Errors, 1)
	assert.True(t, jre.Errors[0].Terminal, "the run error must be terminal")
	assert.NotNil(t, jre.Errors[0].GetLeaseExpired(), "the run error reason must be LeaseExpired")

	var je *armadaevents.JobErrors
	var rq *armadaevents.JobRequeued
	for _, e := range evs {
		if x := e.GetJobErrors(); x != nil {
			je = x
		}
		if x := e.GetJobRequeued(); x != nil {
			rq = x
		}
	}

	require.NotNil(t, je, "a JobErrors event must be emitted")
	require.Len(t, je.Errors, 1)
	assert.False(t, je.Errors[0].Terminal, "the JobErrors must be non-terminal so the api stream sees retryable=true")
	assert.NotNil(t, je.Errors[0].GetLeaseExpired(), "the error reason must be LeaseExpired")

	require.NotNil(t, rq, "a JobRequeued event must be emitted")
	assert.Equal(t, int32(2), rq.UpdateSequenceNumber, "JobRequeued must carry the bumped queued version")

	updated := txn.GetById(job.Id())
	require.NotNil(t, updated)
	assert.True(t, updated.Queued(), "job must be requeued instead of terminally failed")
	assert.False(t, updated.Failed(), "job must not be terminally failed on lease-expiry retry")
	assert.True(t, updated.LatestRun().Failed(), "the expired run must be marked failed")
}

func TestRetryPolicy_FFOn_LeaseExpiryTerminalWhenNoMatch(t *testing.T) {
	policy := mkPolicy(t, 3, api.RetryAction_RETRY_ACTION_FAIL, &api.RetryRule{
		Action:     api.RetryAction_RETRY_ACTION_RETRY,
		OnCategory: "some-other-category",
	})

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"test-policy": policy})
	sched.executorRepository = &testExecutorRepository{
		updateTimes: map[string]time.Time{"testExecutor": sched.clock.Now().Add(-2 * time.Hour)},
	}
	job := makeRunningJobOnExecutor(t, sched)

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	eventSequences, err := sched.expireJobsIfNecessary(armadacontext.Background(), txn)
	require.NoError(t, err)
	require.Len(t, eventSequences, 1)

	// The policy was consulted and declined to retry, so the terminal event is a
	// policy MaxRunsExceeded verdict tagged with the lease-expiry taxonomy rather
	// than a bare LeaseExpired, letting operators tell the two apart.
	terminal := terminalError(eventSequences[0].Events)
	require.NotNil(t, terminal, "lease expiry with no matching rule must emit a terminal error")
	require.NotNil(t, terminal.GetMaxRunsExceeded(), "the terminal error must be a policy MaxRunsExceeded verdict")
	assert.Equal(t, errormatch.CategoryInternal, terminal.FailureCategory, "terminal error must keep the internal failure category")
	assert.Equal(t, errormatch.SubcategoryLeaseExpired, terminal.FailureSubcategory, "terminal error must keep the lease-expired subcategory")

	updated := txn.GetById(job.Id())
	require.NotNil(t, updated)
	assert.True(t, updated.Failed(), "job must be terminally failed when the policy does not match")
}

// makeAttemptedFailedJobForRetry is makeFailedJobForRetry with
// runAttempted=true, so the node anti-affinity path applies on requeue.
func makeAttemptedFailedJobForRetry(t *testing.T, sched *Scheduler) *jobdb.Job {
	t.Helper()
	return makeRetryJob(t, sched, jobRunOpts{schedulingInfo: schedulingInfo, failedRuns: 1, runAttempted: true})
}

// requeuedSchedulingInfo extracts the SchedulingInfo carried by the
// JobRequeued event, failing the test if no requeue event is present.
func requeuedSchedulingInfo(t *testing.T, events []*armadaevents.EventSequence_Event) *schedulerobjects.JobSchedulingInfo {
	t.Helper()
	for _, e := range events {
		if rq := e.GetJobRequeued(); rq != nil {
			return rq.SchedulingInfo
		}
	}
	t.Fatal("expected a JobRequeued event")
	return nil
}

func nodeAntiAffinityValues(si *schedulerobjects.JobSchedulingInfo) []string {
	req := si.GetPodRequirements()
	if req == nil || req.Affinity == nil || req.Affinity.NodeAffinity == nil ||
		req.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return nil
	}
	for _, term := range req.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, me := range term.MatchExpressions {
			if me.Key == nodeIdLabel && me.Operator == v1.NodeSelectorOpNotIn {
				return me.Values
			}
		}
	}
	return nil
}

func TestRetryPolicy_FFOn_EngineRetryAddsNodeAntiAffinity(t *testing.T) {
	policy := mkPolicy(t, 3, api.RetryAction_RETRY_ACTION_FAIL, &api.RetryRule{
		Action:     api.RetryAction_RETRY_ACTION_RETRY,
		OnCategory: "app-error",
	})

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"test-policy": policy})
	job := makeAttemptedFailedJobForRetry(t, sched)

	events, txn := runFailurePath(t, sched, job, categorizedError("app-error"))
	defer txn.Abort()

	si := requeuedSchedulingInfo(t, events.Events)
	assert.Equal(t, []string{"testNode"}, nodeAntiAffinityValues(si),
		"the requeued scheduling info must carry a NotIn anti-affinity for the node the run failed on")
}

// When adding the anti-affinity would make the job unschedulable (e.g. a
// single-node cluster), the retry still happens without it. The legacy path
// fails the job instead.
func TestRetryPolicy_FFOn_EngineRetryRequeuesWithoutAntiAffinityWhenUnschedulable(t *testing.T) {
	policy := mkPolicy(t, 3, api.RetryAction_RETRY_ACTION_FAIL, &api.RetryRule{
		Action:     api.RetryAction_RETRY_ACTION_RETRY,
		OnCategory: "app-error",
	})

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"test-policy": policy})
	sched.submitChecker = &testSubmitChecker{checkSuccess: false}
	job := makeAttemptedFailedJobForRetry(t, sched)

	events, txn := runFailurePath(t, sched, job, categorizedError("app-error"))
	defer txn.Abort()

	hasRequeue, _ := classifyEvents(events.Events)
	assert.True(t, hasRequeue, "engine retry must still requeue when the anti-affinity is unschedulable")
	si := requeuedSchedulingInfo(t, events.Events)
	assert.Empty(t, nodeAntiAffinityValues(si),
		"the fallback requeue must not carry the unschedulable anti-affinity")
}

// The flag-off identity tests below prove, event by event, that with
// retryPolicy.enabled=false the scheduler produces exactly the legacy (flag-off)
// event sequences for a failed run and for an API preemption.

func TestRetryPolicy_FFOff_FailedRunIdentity(t *testing.T) {
	sched := makeRetryTestScheduler(t, false, fakePolicyCache{})
	job := makeFailedJobForRetry(t, sched)

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	runError := containerErrorWithExitCode(42)
	jobErrors := map[string]*armadaevents.Error{job.LatestRun().Id(): runError}

	events, err := sched.generateUpdateMessagesFromJob(armadacontext.Background(), job, jobErrors, nil, txn)
	require.NoError(t, err)
	require.NotNil(t, events)

	// Upstream emits exactly one JobErrors event carrying the run error
	// verbatim for a failed, non-returned run.
	expected := []*armadaevents.EventSequence_Event{
		{
			Created: protoutil.ToTimestamp(sched.clock.Now()),
			Event: &armadaevents.EventSequence_Event_JobErrors{
				JobErrors: &armadaevents.JobErrors{
					JobId:  job.Id(),
					Errors: []*armadaevents.Error{runError},
				},
			},
		},
	}
	assert.Equal(t, expected, events.Events)

	updated := txn.GetById(job.Id())
	require.NotNil(t, updated)
	assert.True(t, updated.Failed())
	assert.False(t, updated.Queued())
}

func TestRetryPolicy_FFOff_ApiPreemptionIdentity(t *testing.T) {
	sched := makeRetryTestScheduler(t, false, fakePolicyCache{})
	job := makePreemptRequestedJobForRetry(t, sched)

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	events, err := sched.generateUpdateMessagesFromJob(armadacontext.Background(), job, nil, nil, txn)
	require.NoError(t, err)
	require.NotNil(t, events)

	expected := createEventsForPreemptedJob(
		job.Id(), job.LatestRun().Id(), "",
		"Preempted - preemption requested via API",
		sched.clock.Now(),
	)
	assert.Equal(t, expected, events.Events)

	updated := txn.GetById(job.Id())
	require.NotNil(t, updated)
	assert.True(t, updated.Failed())
	assert.True(t, updated.LatestRun().Failed())
	assert.False(t, updated.LatestRun().Preempted(),
		"flag off must not pre-mark the run preempted; that lands via the ingester as before")
}

// makeFailFastRunningJobOnExecutor is makeRunningJobOnExecutor with the
// fail-fast annotation set, so the lease-expiry sweep sees a non-gang job that
// opted out of retries.
func makeFailFastRunningJobOnExecutor(t *testing.T, sched *Scheduler) *jobdb.Job {
	t.Helper()
	job := makeRetryJob(t, sched, jobRunOpts{schedulingInfo: failFastSchedulingInfo, leased: true, running: true})
	require.False(t, job.IsInGang(), "fail-fast fixture must not be a gang, so the failFast guard is what excludes it")
	return job
}

func TestRetryPolicy_FFOn_FailFastLeaseExpiryFailsTerminally(t *testing.T) {
	policy := mkPolicy(t, 3, api.RetryAction_RETRY_ACTION_FAIL, &api.RetryRule{
		Action:        api.RetryAction_RETRY_ACTION_RETRY,
		OnCategory:    errormatch.CategoryInternal,
		OnSubcategory: errormatch.SubcategoryLeaseExpired,
	})

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"test-policy": policy})
	sched.executorRepository = &testExecutorRepository{
		updateTimes: map[string]time.Time{"testExecutor": sched.clock.Now().Add(-2 * time.Hour)},
	}
	job := makeFailFastRunningJobOnExecutor(t, sched)

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	_, err := sched.expireJobsIfNecessary(armadacontext.Background(), txn)
	require.NoError(t, err)

	updated := txn.GetById(job.Id())
	require.NotNil(t, updated)
	assert.True(t, updated.Failed(), "fail-fast job must fail terminally on lease expiry even when the policy would retry the category")
	assert.False(t, updated.Queued(), "fail-fast job must not be requeued by the lease-expiry retry path")
}

func TestRetryPolicy_FFOn_FailFastFailurePathFailsTerminally(t *testing.T) {
	policy := mkPolicy(t, 3, api.RetryAction_RETRY_ACTION_RETRY, &api.RetryRule{
		Action:     api.RetryAction_RETRY_ACTION_RETRY,
		OnCategory: "app-error",
	})

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"test-policy": policy})
	job := makeRetryJob(t, sched, jobRunOpts{schedulingInfo: failFastSchedulingInfo, failedRuns: 1})

	events, txn := runFailurePath(t, sched, job, categorizedError("app-error"))
	defer txn.Abort()

	hasRequeue, _ := classifyEvents(events.Events)
	assert.False(t, hasRequeue, "fail-fast job must not be requeued even when a retry rule matches")

	// Fail-fast bypasses the engine entirely, so the original run error is
	// emitted verbatim rather than rewritten into a policy MaxRunsExceeded verdict.
	var emitted *armadaevents.Error
	for _, e := range events.Events {
		if je := e.GetJobErrors(); je != nil && len(je.Errors) > 0 {
			emitted = je.Errors[len(je.Errors)-1]
		}
	}
	require.NotNil(t, emitted, "fail-fast job must emit its failure event")
	assert.NotNil(t, emitted.GetPodError(), "the original pod error must be preserved")
	assert.Nil(t, emitted.GetMaxRunsExceeded(), "the engine must not rewrite a fail-fast failure")

	updated := txn.GetById(job.Id())
	require.NotNil(t, updated)
	assert.True(t, updated.Failed(), "fail-fast job must be terminally failed")
	assert.False(t, updated.Queued())
}

func TestRetryPolicy_FFOn_GangSkipIncrementsMetricAndDoesNotRetry(t *testing.T) {
	policy := mkPolicy(t, 3, api.RetryAction_RETRY_ACTION_RETRY, &api.RetryRule{
		Action:     api.RetryAction_RETRY_ACTION_RETRY,
		OnCategory: "app-error",
	})

	sched := makeRetryTestScheduler(t, true, fakePolicyCache{"test-policy": policy})
	job := makeRetryJob(t, sched, jobRunOpts{schedulingInfo: preemptibleGangSchedulingInfo, failedRuns: 1})
	require.True(t, job.IsInGang(), "gang fixture must be a gang for this test to exercise the gang-skip branch")

	before := testutil.ToFloat64(retryPolicyGangSkippedCounter.WithLabelValues("test-policy"))
	events, txn := runFailurePath(t, sched, job, categorizedError("app-error"))
	defer txn.Abort()

	hasRequeue, _ := classifyEvents(events.Events)
	assert.False(t, hasRequeue, "a gang job must never be retried by the engine even when a rule matches")
	after := testutil.ToFloat64(retryPolicyGangSkippedCounter.WithLabelValues("test-policy"))
	assert.Equal(t, before+1, after, "the gang skip must increment the counter for the attached policy")
}

func TestRetryPolicy_FFOff_LeaseExpiryIdentity(t *testing.T) {
	sched := makeRetryTestScheduler(t, false, fakePolicyCache{})
	sched.executorRepository = &testExecutorRepository{
		updateTimes: map[string]time.Time{"testExecutor": sched.clock.Now().Add(-2 * time.Hour)},
	}
	job := makeRunningJobOnExecutor(t, sched)

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	eventSequences, err := sched.expireJobsIfNecessary(armadacontext.Background(), txn)
	require.NoError(t, err)
	require.Len(t, eventSequences, 1)

	expected := createEventsForFailedJob(
		job.Id(), job.LatestRun().Id(),
		&armadaevents.Error{
			Terminal:           true,
			FailureCategory:    errormatch.CategoryInternal,
			FailureSubcategory: errormatch.SubcategoryLeaseExpired,
			Reason: &armadaevents.Error_LeaseExpired{
				LeaseExpired: &armadaevents.LeaseExpired{},
			},
		},
		sched.clock.Now(),
	)
	assert.Equal(t, expected, eventSequences[0].Events,
		"flag off must emit the legacy (flag-off) lease-expiry failure events unchanged")

	updated := txn.GetById(job.Id())
	require.NotNil(t, updated)
	assert.True(t, updated.Failed(), "flag off must terminally fail the lease-expired job")
	assert.False(t, updated.Queued())
}

// The two gating tests below populate a cache with a policy that would retry, so
// they fail if the feature-flag guard is removed from either engine call site.

func TestRetryPolicy_FFOff_FailurePathIgnoresPopulatedPolicy(t *testing.T) {
	policy := mkPolicy(t, 3, api.RetryAction_RETRY_ACTION_RETRY, &api.RetryRule{
		Action:     api.RetryAction_RETRY_ACTION_RETRY,
		OnCategory: "app-error",
	})
	sched := makeRetryTestScheduler(t, false, fakePolicyCache{"test-policy": policy})
	job := makeFailedJobForRetry(t, sched)

	events, txn := runFailurePath(t, sched, job, categorizedError("app-error"))
	defer txn.Abort()

	hasRequeue, _ := classifyEvents(events.Events)
	assert.False(t, hasRequeue, "flag off must not consult the engine, so a cached Retry policy must not requeue the job")
	updated := txn.GetById(job.Id())
	require.NotNil(t, updated)
	assert.True(t, updated.Failed(), "flag off must terminally fail the job via the legacy path despite the cached Retry policy")
}

func TestRetryPolicy_FFOff_LeaseExpiryIgnoresPopulatedPolicy(t *testing.T) {
	policy := mkPolicy(t, 3, api.RetryAction_RETRY_ACTION_FAIL, &api.RetryRule{
		Action:        api.RetryAction_RETRY_ACTION_RETRY,
		OnCategory:    errormatch.CategoryInternal,
		OnSubcategory: errormatch.SubcategoryLeaseExpired,
	})
	sched := makeRetryTestScheduler(t, false, fakePolicyCache{"test-policy": policy})
	// DefaultPolicyName makes the policy resolvable on the lease-expiry path
	// without the flag-gated queue map, so only the Enabled guard can stop it.
	sched.retryPolicyConfig.DefaultPolicyName = "test-policy"
	sched.executorRepository = &testExecutorRepository{
		updateTimes: map[string]time.Time{"testExecutor": sched.clock.Now().Add(-2 * time.Hour)},
	}
	job := makeRunningJobOnExecutor(t, sched)

	txn := sched.jobDb.WriteTxn()
	defer txn.Abort()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	_, err := sched.expireJobsIfNecessary(armadacontext.Background(), txn)
	require.NoError(t, err)

	updated := txn.GetById(job.Id())
	require.NotNil(t, updated)
	assert.True(t, updated.Failed(), "flag off must terminally fail the lease-expired job even when the default policy would retry it")
	assert.False(t, updated.Queued(), "flag off must not requeue via the engine on lease expiry")
}
