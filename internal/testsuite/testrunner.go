package testsuite

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/testsuite/eventbenchmark"
	"github.com/armadaproject/armada/internal/testsuite/eventlogger"
	"github.com/armadaproject/armada/internal/testsuite/eventsplitter"
	"github.com/armadaproject/armada/internal/testsuite/eventwatcher"
	"github.com/armadaproject/armada/internal/testsuite/queue"
	"github.com/armadaproject/armada/internal/testsuite/submitter"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// Check a call's outcome is reconciled against testSpec.ExpectErrorCode for negative tests.
func checkExpectedError(testSpec *api.TestSpec, err error) error {
	if err == nil || testSpec.GetExpectedError().GetCode() == 0 {
		return err
	}
	wantCode := codes.Code(testSpec.GetExpectedError().GetCode())
	// A plain error (e.g. failing to reach the auth provider) maps to codes.Unknown, so only
	// accept errors that are a real gRPC status returned by the server.
	if _, ok := status.FromError(err); !ok {
		return errors.Wrapf(err, "expected call to fail with a gRPC status of %s, got a non-gRPC error", wantCode)
	}
	if status.Code(err) != wantCode {
		return errors.Wrapf(err, "expected call to fail with code %s", wantCode)
	}
	return nil
}

// validateExpectedError requires the phase to be stated whenever an error is expected.
func validateExpectedError(testSpec *api.TestSpec) error {
	expected := testSpec.GetExpectedError()
	if expected.GetCode() != 0 && expected.GetPhase() == api.ExpectedErrorConfig_UNSPECIFIED {
		return errors.New("expectedError.phase must be set (QUEUE, SUBMIT or ACTION) when expectedError.code is set")
	}
	return nil
}

// reconcileError checks the outcome of a call made during the given phase. The expected error code
// is only accepted from the phase under test, so a permission failure in an earlier setup step can't
// satisfy a test that targets a later action. observed is true if the expected error was seen.
func reconcileError(testSpec *api.TestSpec, under, current api.ExpectedErrorConfig_Phase, err error) (observed bool, outErr error) {
	if err == nil {
		return false, nil
	}
	if under != current {
		return false, err
	}
	if outErr = checkExpectedError(testSpec, err); outErr != nil || testSpec.GetExpectedError().GetCode() == 0 {
		return false, outErr
	}
	return true, nil
}

type TestRunner struct {
	Out                  io.Writer
	apiConnectionDetails *client.ApiConnectionDetails
	// used to authenticate cleanup actions at the end of the test run.
	cleanupConnectionDetails *client.ApiConnectionDetails
	testSpec                 *api.TestSpec
	eventLogger              *eventlogger.EventLogger
	TestCaseReport           *TestCaseReport
}

func (srv *TestRunner) cleanupConn() *client.ApiConnectionDetails {
	if srv.cleanupConnectionDetails != nil {
		return srv.cleanupConnectionDetails
	}
	return srv.apiConnectionDetails
}

// Convert to Junit TestCase according to spec: https://llg.cubic.org/docs/junit/
func (report *TestCaseReport) JunitTestCase() junit.Testcase {
	var failure *junit.Result
	if report.FailureReason != "" {
		failure = &junit.Result{
			Message: report.FailureReason,
		}
	}
	return junit.Testcase{
		Name:      report.TestSpec.Name,
		Classname: report.TestSpec.Name,
		Time:      report.Finish.Sub(report.Start).String(),
		Failure:   failure,
		SystemOut: &junit.Output{
			Data: report.Out.String(),
		},
	}
}

func (srv *TestRunner) Run(ctx context.Context) (err error) {
	report := NewTestCaseReport(srv.testSpec)
	report.Out = &bytes.Buffer{}
	out := io.MultiWriter(srv.Out, report.Out)

	fmt.Fprintf(out, "test case started %s\n", srv.testSpec.ShortString())
	defer func() {
		report.Finish = time.Now()
		srv.TestCaseReport = report
		if err != nil {
			report.FailureReason = err.Error()
			fmt.Fprintf(out, "test case %s failed: %s\n", srv.testSpec.Name, report.FailureReason)
		} else {
			fmt.Fprintf(out, "test case %s succeeded\n", srv.testSpec.Name)
		}
	}()

	// Track whether an expected error has been observed during the test run.
	var expectedErrorObserved bool
	defer func() {
		if err == nil && srv.testSpec.GetExpectedError().GetCode() != 0 && !expectedErrorObserved {
			err = errors.Errorf(
				"expected a call to fail with code %s, but the test completed successfully",
				codes.Code(srv.testSpec.GetExpectedError().GetCode()),
			)
		}
	}()

	if err = validateExpectedError(srv.testSpec); err != nil {
		return err
	}

	// Optional timeout
	var cancel context.CancelFunc
	timeout := protoutil.ToStdDuration(srv.testSpec.Timeout)
	if timeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	// Create and (optionally) update the queue(s) under test.
	// Queues are fixtures unless the queue calls themselves are under test, so create and delete
	// them as the privileged principal rather than the (possibly restricted) principal under test.
	phaseUnderTest := srv.testSpec.GetExpectedError().GetPhase()
	queueConn := srv.apiConnectionDetails
	if phaseUnderTest == api.ExpectedErrorConfig_SUBMIT || phaseUnderTest == api.ExpectedErrorConfig_ACTION {
		queueConn = srv.cleanupConn()
	}

	queueNames, setupErr := queue.RunSetup(ctx, srv.testSpec, queueConn, out)
	observed, err := reconcileError(srv.testSpec, phaseUnderTest, api.ExpectedErrorConfig_QUEUE, setupErr)
	if err != nil {
		return err
	}
	if observed {
		expectedErrorObserved = true
		return nil
	}
	updateErr := queue.RunUpdate(ctx, queueNames, srv.testSpec, queueConn, out)
	observed, err = reconcileError(srv.testSpec, phaseUnderTest, api.ExpectedErrorConfig_QUEUE, updateErr)
	if err != nil {
		return err
	}
	if observed {
		expectedErrorObserved = true
		return nil
	}

	// (deferred): always delete the queue(s) once the test finishes.
	defer func() {
		teardownErr := queue.RunTeardown(queueNames, srv.testSpec, queueConn, out)
		observed, checkedErr := reconcileError(srv.testSpec, phaseUnderTest, api.ExpectedErrorConfig_QUEUE, teardownErr)
		if checkedErr != nil {
			fmt.Fprintf(out, "warning: queue teardown failed: %s\n", checkedErr)
			if err == nil {
				err = checkedErr
			}
			return
		}
		if observed {
			expectedErrorObserved = true
		}
	}()

	// Pure queue tests submit no jobs, so skip the job-submission block below and
	// go straight to the queue assertions.
	if len(srv.testSpec.Jobs) == 0 && srv.testSpec.NumBatches == 0 {
		return queue.RunAssertions(ctx, queueNames, srv.testSpec, srv.apiConnectionDetails, out)
	}

	// Setup an errgroup that cancels on any job failing or there being no active jobs.
	g, ctx := errgroup.WithContext(ctx)

	// Submit jobs. All jobs must be submitted before proceeding since we need the job ids.
	sbmtr := submitter.NewSubmitterFromTestSpec(srv.apiConnectionDetails, srv.testSpec, out)
	submitErr := sbmtr.Run(ctx)
	observed, err = reconcileError(srv.testSpec, phaseUnderTest, api.ExpectedErrorConfig_SUBMIT, submitErr)
	if err != nil {
		return err
	}
	if observed {
		expectedErrorObserved = true
		return nil
	}
	// Before returning, cancel the job set to ensure there are no lingering jobs, including when
	// a negative submit test unexpectedly succeeds.
	defer func() {
		if err := cancelJobSet(srv.cleanupConn(), srv.testSpec.Queue, srv.testSpec.JobSetId); err != nil {
			fmt.Fprintf(out, "failed to cancel job set %s: %s\n", srv.testSpec.JobSetId, err)
		}
	}()

	if phaseUnderTest == api.ExpectedErrorConfig_SUBMIT {
		return errors.Errorf(
			"expected job submission to fail with code %s, but it succeeded",
			codes.Code(srv.testSpec.GetExpectedError().GetCode()),
		)
	}
	jobIds := sbmtr.JobIds()
	jobIdMap := make(map[string]bool)
	for _, jobId := range jobIds {
		jobIdMap[jobId] = false
	}

	nodeName, err := resolveNodeName(ctx, srv.testSpec)
	if err != nil {
		return err
	}

	// One channel for each system listening to events.
	benchmarkCh := make(chan *api.EventMessage)
	noActiveCh := make(chan *api.EventMessage)
	assertCh := make(chan *api.EventMessage)
	ingressCh := make(chan *api.EventMessage)

	// actionUnderTestErr holds the result of whichever RPC a negative RBAC test is checking
	var actionUnderTestErr error
	actionConfigured := hasConfiguredAction(srv.testSpec)

	// Goroutine forwarding API events on a channel.
	watcher := eventwatcher.New(srv.testSpec.Queue, srv.testSpec.JobSetId, srv.apiConnectionDetails)
	watcher.Out = out
	g.Go(func() error {
		err := watcher.Run(ctx)
		// Only the RPC under test writes actionUnderTestErr; when an action is configured
		if !actionConfigured {
			actionUnderTestErr = err
		}
		return err
	})

	// Build list of event channels based on test configuration.
	eventChannels := []chan *api.EventMessage{assertCh, ingressCh, noActiveCh, benchmarkCh, srv.eventLogger.In}

	// Add action channel if cancel or preempt is configured and waits for all jobs to reach a trigger state before acting.
	var actionCh chan *api.EventMessage
	if actionConfigured {
		actionCh = make(chan *api.EventMessage)
		eventChannels = append(eventChannels, actionCh)
	}

	// Duplicate events across all downstream services.
	splitter := eventsplitter.New(
		watcher.C,
		eventChannels...,
	)
	g.Go(func() error { return splitter.Run(ctx) })

	// If configured, cancel or preempt jobs once all reach the configured trigger event.
	if actionConfigured {
		extractor, err := triggerEventExtractor(srv.testSpec)
		if err != nil {
			return err
		}
		g.Go(func() error {
			err := runActionOnState(ctx, out, actionCh, srv.testSpec, srv.apiConnectionDetails, jobIds, nodeName, extractor)
			actionUnderTestErr = err
			return err
		})
	}

	// Cancel the errgroup if there are no active jobs.
	g.Go(func() error { return eventwatcher.ErrorOnNoActiveJobs(ctx, noActiveCh, maps.Clone(jobIdMap)) })

	// Record time spent per job state. Used to benchmark jobs.
	eventBenchmark := eventbenchmark.New(benchmarkCh)
	eventBenchmark.Out = out
	g.Go(func() error { return eventBenchmark.Run(ctx) })
	defer func() {
		report.BenchmarkReport = eventBenchmark.NewTestCaseBenchmarkReport(srv.testSpec.GetName())
	}()

	// Watch for ingress events and try to download from any ingresses found.
	g.Go(func() error { return eventwatcher.GetFromIngresses(ctx, ingressCh) })

	if srv.testSpec.GetExpectedError().GetCode() != 0 {
		_ = eventwatcher.AssertEvents(ctx, assertCh, maps.Clone(jobIdMap), srv.testSpec.ExpectedEvents)
		cancel()
		_ = g.Wait()
		observed, checkedErr := reconcileError(srv.testSpec, phaseUnderTest, api.ExpectedErrorConfig_ACTION, actionUnderTestErr)
		if checkedErr != nil {
			return checkedErr
		}
		expectedErrorObserved = observed
		return nil
	}

	// Assert that we get the right events for each job.
	// Returns once we've received all events or when ctx is cancelled.
	if assertErr := eventwatcher.AssertEvents(ctx, assertCh, maps.Clone(jobIdMap), srv.testSpec.ExpectedEvents); assertErr != nil {
		cancel()
		groupErr := g.Wait()
		if groupErr != nil {
			return errors.Errorf("%s: %s", assertErr, groupErr)
		}
		return assertErr
	}

	// If configured, cancel the job set once it has no active jobs (all jobs terminal) and
	// assert that cancel is a successful no-op. Runs only after AssertEvents confirmed every
	// job reached a terminal state.
	if srv.testSpec.CancelJobSet.GetAssertInactiveSucceeds() {
		if err = cancelJobSet(srv.apiConnectionDetails, srv.testSpec.Queue, srv.testSpec.JobSetId); err != nil {
			return errors.WithMessage(err, "cancel of job set with no active jobs returned an error")
		}
	}

	// Assert queue state now that the jobs have finished. (used for GetActiveQueues)
	return queue.RunAssertions(ctx, queueNames, srv.testSpec, srv.apiConnectionDetails, out)
}

// hasConfiguredAction reports whether testSpec configures a cancel/preempt/reprioritize action,
// i.e. whether an action should be triggered once all jobs reach the configured trigger event.
func hasConfiguredAction(testSpec *api.TestSpec) bool {
	return testSpec.Cancel != nil || testSpec.CancelJobSet != nil || testSpec.Preempt != nil ||
		testSpec.Reprioritize != nil || testSpec.CancelOnNode != nil || testSpec.PreemptOnNode != nil
}

// triggerEventExtractor resolves testSpec.TriggerEvent to an extractor function.
// If TriggerEvent is unset, falls back to the default behavior:
//   - Running for PREEMPT/REPRIORITIZE and node-scoped operations (CancelOnNode/PreemptOnNode)
//   - Queued for CANCEL via the submit API (BY_ID, BY_IDS, BY_SET, CancelJobSet), which works from any state.
func triggerEventExtractor(testSpec *api.TestSpec) (func(*api.EventMessage) string, error) {
	name := testSpec.TriggerEvent
	if name == "" {
		if testSpec.CancelOnNode != nil || testSpec.PreemptOnNode != nil ||
			testSpec.Preempt != nil || testSpec.Reprioritize != nil {
			name = "running"
		} else {
			name = "queued"
		}
	}
	extractor, ok := triggerEventExtractors[name]
	if !ok {
		return nil, errors.Errorf("unknown triggerEvent %q", name)
	}
	return extractor, nil
}

// triggerEventExtractors maps a TestSpec.TriggerEvent name to a function that extracts
// the job ID from an EventMessage if it matches that event, or "" otherwise.
// Names match the EventMessage oneof field names (see pkg/api/event.proto), which correspond
// to the external API events in docs/developer/job-lifecycle-events.md#event-vocabulary.
var triggerEventExtractors = map[string]func(*api.EventMessage) string{
	"submitted": func(msg *api.EventMessage) string {
		if e := msg.GetSubmitted(); e != nil {
			return e.JobId
		}
		return ""
	},
	"queued": func(msg *api.EventMessage) string {
		if e := msg.GetQueued(); e != nil {
			return e.JobId
		}
		return ""
	},
	"leased": func(msg *api.EventMessage) string {
		if e := msg.GetLeased(); e != nil {
			return e.JobId
		}
		return ""
	},
	"running": func(msg *api.EventMessage) string {
		if e := msg.GetRunning(); e != nil {
			return e.JobId
		}
		return ""
	},
	"succeeded": func(msg *api.EventMessage) string {
		if e := msg.GetSucceeded(); e != nil {
			return e.JobId
		}
		return ""
	},
	"failed": func(msg *api.EventMessage) string {
		if e := msg.GetFailed(); e != nil {
			return e.JobId
		}
		return ""
	},
	"cancelled": func(msg *api.EventMessage) string {
		if e := msg.GetCancelled(); e != nil {
			return e.JobId
		}
		return ""
	},
	"preempted": func(msg *api.EventMessage) string {
		if e := msg.GetPreempted(); e != nil {
			return e.JobId
		}
		return ""
	},
}

// cancelJobSet cancels a job set. Uses a fresh context so it is unaffected by the errgroup
// ctx, which is cancelled once all jobs reach a terminal state.
func cancelJobSet(conn *client.ApiConnectionDetails, queue, jobSetId string) error {
	return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := sc.CancelJobSet(ctx, &api.JobSetCancelRequest{Queue: queue, JobSetId: jobSetId})
		return errors.WithStack(err)
	})
}

// actionName returns a human-readable label for testSpec's configured action, for logging
// alongside its outcome (see runActionOnState) -- mirrors dispatchAction's own switch so the
// label always matches the RPC actually issued.
func actionName(testSpec *api.TestSpec) string {
	switch {
	case testSpec.CancelJobSet != nil:
		return "CancelJobSet"
	case testSpec.CancelOnNode != nil:
		return "CancelOnNode"
	case testSpec.PreemptOnNode != nil:
		return "PreemptOnNode"
	case testSpec.Cancel != nil:
		return "CancelJobs"
	case testSpec.Preempt != nil:
		return "PreemptJobs"
	case testSpec.Reprioritize != nil:
		return "ReprioritizeJobs"
	default:
		return "action"
	}
}

// runActionOnState waits for all jobs to be reported by jobIdFromEvent, then issues the configured action.
// jobIdFromEvent should return the job ID when the event matches the desired trigger state, or "" to ignore the event.
// nodeName is only relevant for node-scoped actions (CancelOnNode/PreemptOnNode)
func runActionOnState(ctx context.Context, out io.Writer, eventCh chan *api.EventMessage, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, jobIds []string, nodeName string, jobIdFromEvent func(*api.EventMessage) string) error {
	jobIdSet := make(map[string]bool, len(jobIds))
	for _, id := range jobIds {
		jobIdSet[id] = true
	}
	triggeredJobs := make(map[string]bool)
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-eventCh:
			if jobId := jobIdFromEvent(msg); jobId != "" && jobIdSet[jobId] {
				triggeredJobs[jobId] = true
				if len(triggeredJobs) == len(jobIds) {
					time.Sleep(1 * time.Second)
					err := dispatchAction(ctx, testSpec, conn, jobIds, nodeName)
					if err != nil {
						fmt.Fprintf(out, "%s %d job(s) in job set %s: FAIL (%v)\n", actionName(testSpec), len(jobIds), testSpec.JobSetId, err)
						return err
					}
					fmt.Fprintf(out, "%s %d job(s) in job set %s: SUCCESS\n", actionName(testSpec), len(jobIds), testSpec.JobSetId)
					// Drain the channel to avoid blocking the splitter.
					for {
						select {
						case <-ctx.Done():
							return nil
						case <-eventCh:
						}
					}
				}
			}
		}
	}
}

// dispatchAction issues the action configured on testSpec (Cancel, CancelJobSet, Preempt,
// Reprioritize, CancelOnNode, or PreemptOnNode). nodeName is only relevant for node-scoped actions.
func dispatchAction(ctx context.Context, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, jobIds []string, nodeName string) error {
	switch {
	case testSpec.CancelJobSet != nil:
		return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
			req := testSpec.CancelJobSet.GetRequest()
			if req == nil {
				req = &api.JobSetCancelRequest{}
			}
			req.Queue = testSpec.GetQueue()
			req.JobSetId = testSpec.GetJobSetId()
			_, err := sc.CancelJobSet(ctx, req)
			return errors.WithStack(err)
		})
	case testSpec.CancelOnNode != nil:
		return client.WithNodeClient(conn, func(nc api.NodeClient) error {
			req := testSpec.CancelOnNode.GetRequest()
			if req == nil {
				req = &api.NodeCancelRequest{}
			}
			req.Name = nodeName
			_, err := nc.CancelOnNode(ctx, req)
			return errors.WithStack(err)
		})
	case testSpec.PreemptOnNode != nil:
		return client.WithNodeClient(conn, func(nc api.NodeClient) error {
			req := testSpec.PreemptOnNode.GetRequest()
			if req == nil {
				req = &api.NodePreemptRequest{}
			}
			req.Name = nodeName
			_, err := nc.PreemptOnNode(ctx, req)
			return errors.WithStack(err)
		})
	case testSpec.Cancel != nil:
		return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
			req := testSpec.Cancel.GetRequest()
			if req == nil {
				req = &api.JobCancelRequest{}
			}
			req.Queue = testSpec.GetQueue()
			req.JobSetId = testSpec.GetJobSetId()
			req.JobIds = jobIds
			_, err := sc.CancelJobs(ctx, req)
			return errors.WithStack(err)
		})
	case testSpec.Preempt != nil:
		return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
			req := testSpec.Preempt.GetRequest()
			if req == nil {
				req = &api.JobPreemptRequest{}
			}
			req.Queue = testSpec.GetQueue()
			req.JobSetId = testSpec.GetJobSetId()
			req.JobIds = jobIds
			_, err := sc.PreemptJobs(ctx, req)
			return errors.WithStack(err)
		})
	case testSpec.Reprioritize != nil:
		return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
			req := testSpec.Reprioritize.GetRequest()
			if req == nil {
				req = &api.JobReprioritizeRequest{}
			}
			req.Queue = testSpec.GetQueue()
			req.JobSetId = testSpec.GetJobSetId()
			if !testSpec.Reprioritize.GetByJobSet() {
				req.JobIds = jobIds
			}
			_, err := sc.ReprioritizeJobs(ctx, req)
			return errors.WithStack(err)
		})
	default:
		return errors.Errorf("no action configured for test spec %s", testSpec.GetName())
	}
}

// resolveNodeName resolves the target node name for testSpec's node-scoped action
// (CancelOnNode or PreemptOnNode), or "" if neither is configured.
func resolveNodeName(ctx context.Context, testSpec *api.TestSpec) (string, error) {
	switch {
	case testSpec.CancelOnNode != nil:
		return resolveNodeByPoolTag(ctx, testSpec.CancelOnNode.NodePoolTag)
	case testSpec.PreemptOnNode != nil:
		return resolveNodeByPoolTag(ctx, testSpec.PreemptOnNode.NodePoolTag)
	default:
		return "", nil
	}
}

// resolveNodeByPoolTag finds the k8s node name for the given armadaproject.io/node-pool label value.
// Node-scoped api calls need the node name, which kind sets dynamically.
func resolveNodeByPoolTag(ctx context.Context, tag string) (string, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, nil).ClientConfig()
	if err != nil {
		return "", errors.Wrap(err, "failed to load kubeconfig")
	}
	k8sClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", errors.Wrap(err, "failed to create k8s client")
	}
	labelSelector := fmt.Sprintf("armadaproject.io/node-pool=%s", tag)
	nodes, err := k8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return "", errors.Wrapf(err, "failed to list nodes with label %s", labelSelector)
	}
	if len(nodes.Items) == 0 {
		return "", errors.Errorf("no node found with label %s", labelSelector)
	}
	if len(nodes.Items) > 1 {
		fmt.Printf("warn: multiple nodes match label %s; using %s\n", labelSelector, nodes.Items[0].Name)
	}
	return nodes.Items[0].Name, nil
}
