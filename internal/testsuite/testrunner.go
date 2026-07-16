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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/testsuite/eventbenchmark"
	"github.com/armadaproject/armada/internal/testsuite/eventlogger"
	"github.com/armadaproject/armada/internal/testsuite/eventsplitter"
	"github.com/armadaproject/armada/internal/testsuite/eventwatcher"
	"github.com/armadaproject/armada/internal/testsuite/submitter"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

type TestRunner struct {
	Out                  io.Writer
	apiConnectionDetails *client.ApiConnectionDetails
	testSpec             *api.TestSpec
	eventLogger          *eventlogger.EventLogger
	TestCaseReport       *TestCaseReport
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

	// Optional timeout
	var cancel context.CancelFunc
	timeout := protoutil.ToStdDuration(srv.testSpec.Timeout)
	if timeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	// Setup an errgroup that cancels on any job failing or there being no active jobs.
	g, ctx := errgroup.WithContext(ctx)

	// Submit jobs. All jobs must be submitted before proceeding since we need the job ids.
	sbmtr := submitter.NewSubmitterFromTestSpec(srv.apiConnectionDetails, srv.testSpec, out)
	if err = sbmtr.Run(ctx); err != nil {
		return err
	}
	jobIds := sbmtr.JobIds()
	jobIdMap := make(map[string]bool)
	for _, jobId := range jobIds {
		jobIdMap[jobId] = false
	}

	nodeName := ""
	if srv.testSpec.Selection == api.TestSpec_SELECTION_BY_NODE {
		nodeName, err = resolveNodeByPoolTag(ctx, srv.testSpec.NodePoolTag)
		if err != nil {
			return err
		}
	}

	// Before returning, cancel the job set to ensure there are no lingering jobs.
	defer func() {
		err := client.WithSubmitClient(srv.apiConnectionDetails, func(sc api.SubmitClient) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := sc.CancelJobSet(ctx, &api.JobSetCancelRequest{
				JobSetId: srv.testSpec.JobSetId,
				Queue:    srv.testSpec.Queue,
			})
			return err
		})
		if err != nil {
			fmt.Fprintf(out, "failed to cancel job set %s: %s\n", srv.testSpec.JobSetId, err)
		}
	}()

	// One channel for each system listening to events.
	benchmarkCh := make(chan *api.EventMessage)
	noActiveCh := make(chan *api.EventMessage)
	assertCh := make(chan *api.EventMessage)
	ingressCh := make(chan *api.EventMessage)

	// Goroutine forwarding API events on a channel.
	watcher := eventwatcher.New(srv.testSpec.Queue, srv.testSpec.JobSetId, srv.apiConnectionDetails)
	watcher.Out = out
	g.Go(func() error { return watcher.Run(ctx) })

	// Build list of event channels based on test configuration.
	eventChannels := []chan *api.EventMessage{assertCh, ingressCh, noActiveCh, benchmarkCh, srv.eventLogger.In}

	// Add action channel if cancel or preempt is configured — waits for all jobs to reach a trigger state before acting.
	var actionCh chan *api.EventMessage
	if srv.testSpec.Action == api.TestSpec_ACTION_CANCEL || srv.testSpec.Action == api.TestSpec_ACTION_PREEMPT {
		actionCh = make(chan *api.EventMessage)
		eventChannels = append(eventChannels, actionCh)
	}

	// Duplicate events across all downstream services.
	splitter := eventsplitter.New(
		watcher.C,
		eventChannels...,
	)
	g.Go(func() error { return splitter.Run(ctx) })

	// If configured, cancel or preempt jobs once all reach the appropriate trigger state.
	// - Node-scoped operations require Running (SQL query constraint).
	// - Preempt requires Running (preemption acts on the job run, which only exists once running).
	// - Cancel via submit API can fire as soon as Queued.
	if srv.testSpec.Action == api.TestSpec_ACTION_CANCEL || srv.testSpec.Action == api.TestSpec_ACTION_PREEMPT {
		g.Go(func() error {
			if srv.testSpec.Selection == api.TestSpec_SELECTION_BY_NODE || srv.testSpec.Action == api.TestSpec_ACTION_PREEMPT {
				return runActionWhenRunning(ctx, actionCh, srv.testSpec, srv.apiConnectionDetails, jobIds, nodeName)
			}
			return runActionWhenQueued(ctx, actionCh, srv.testSpec, srv.apiConnectionDetails, jobIds, nodeName)
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

	// Assert that we get the right events for each job.
	// Returns once we've received all events or when ctx is cancelled.
	if err = eventwatcher.AssertEvents(ctx, assertCh, maps.Clone(jobIdMap), srv.testSpec.ExpectedEvents); err != nil {
		cancel()
		groupErr := g.Wait()
		if groupErr != nil {
			return errors.Errorf("%s: %s", err, groupErr)
		} else {
			return err
		}
	}
	return nil
}

// runActionWhenRunning waits for all jobs to reach the Running state, then issues the configured action.
// Required for node-scoped operations: CancelOnNode/PreemptOnNode only match jobs currently running on the node.
func runActionWhenRunning(ctx context.Context, eventCh chan *api.EventMessage, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, jobIds []string, nodeName string) error {
	return runActionOnState(ctx, eventCh, testSpec, conn, jobIds, nodeName,
		func(msg *api.EventMessage) string {
			if e := msg.GetRunning(); e != nil {
				return e.JobId
			}
			return ""
		},
	)
}

// runActionWhenQueued waits for all jobs to reach the Queued state, then issues the configured action.
// Suitable for submit-API cancel/preempt (BY_ID, BY_IDS, BY_SET) which work from any state.
func runActionWhenQueued(ctx context.Context, eventCh chan *api.EventMessage, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, jobIds []string, nodeName string) error {
	return runActionOnState(ctx, eventCh, testSpec, conn, jobIds, nodeName,
		func(msg *api.EventMessage) string {
			if e := msg.GetQueued(); e != nil {
				return e.JobId
			}
			return ""
		},
	)
}

// runActionOnState waits for all jobs to be reported by jobIdFromEvent, then issues the configured action.
// jobIdFromEvent should return the job ID when the event matches the desired trigger state, or "" to ignore the event.
// Not every event needs all of these parameters, for instance nodeName is only relevant for node-scoped actions.
// This is a consequence of a testSpec.proto being too long. Ideally we will modularize the testspec in future prs.
func runActionOnState(ctx context.Context, eventCh chan *api.EventMessage, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, jobIds []string, nodeName string, jobIdFromEvent func(*api.EventMessage) string) error {
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
					var actionErr error
					switch {
					case testSpec.Action == api.TestSpec_ACTION_CANCEL && testSpec.Selection == api.TestSpec_SELECTION_BY_NODE:
						actionErr = client.WithNodeClient(conn, func(nc api.NodeClient) error {
							_, err := nc.CancelOnNode(ctx, &api.NodeCancelRequest{
								Name:     nodeName,
								Executor: testSpec.GetExecutor(),
								Queues:   []string{testSpec.GetQueue()},
							})
							return errors.WithStack(err)
						})
					case testSpec.Action == api.TestSpec_ACTION_CANCEL && testSpec.Selection == api.TestSpec_SELECTION_BY_ID:
						actionErr = client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
							req := &api.JobCancelRequest{Queue: testSpec.GetQueue(), JobSetId: testSpec.GetJobSetId()}
							for _, jobId := range jobIds {
								req.JobId = jobId
								if _, err := sc.CancelJobs(ctx, req); err != nil {
									return errors.WithStack(err)
								}
							}
							return nil
						})
					case testSpec.Action == api.TestSpec_ACTION_CANCEL && testSpec.Selection == api.TestSpec_SELECTION_BY_IDS:
						actionErr = client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
							_, err := sc.CancelJobs(ctx, &api.JobCancelRequest{
								Queue:    testSpec.GetQueue(),
								JobSetId: testSpec.GetJobSetId(),
								JobIds:   jobIds,
							})
							return errors.WithStack(err)
						})
					case testSpec.Action == api.TestSpec_ACTION_CANCEL && testSpec.Selection == api.TestSpec_SELECTION_BY_SET:
						actionErr = client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
							_, err := sc.CancelJobs(ctx, &api.JobCancelRequest{
								Queue:    testSpec.GetQueue(),
								JobSetId: testSpec.GetJobSetId(),
							})
							return errors.WithStack(err)
						})
					case testSpec.Action == api.TestSpec_ACTION_PREEMPT && testSpec.Selection == api.TestSpec_SELECTION_BY_NODE:
						actionErr = client.WithNodeClient(conn, func(nc api.NodeClient) error {
							_, err := nc.PreemptOnNode(ctx, &api.NodePreemptRequest{
								Name:     nodeName,
								Executor: testSpec.GetExecutor(),
								Queues:   []string{testSpec.GetQueue()},
							})
							return errors.WithStack(err)
						})
					case testSpec.Action == api.TestSpec_ACTION_PREEMPT && testSpec.Selection == api.TestSpec_SELECTION_BY_ID:
						actionErr = client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
							req := &api.JobPreemptRequest{Queue: testSpec.GetQueue(), JobSetId: testSpec.GetJobSetId(), Reason: testSpec.GetPreemptReason()}
							for _, jobId := range jobIds {
								req.JobIds = []string{jobId}
								if _, err := sc.PreemptJobs(ctx, req); err != nil {
									return errors.WithStack(err)
								}
							}
							return nil
						})
					case testSpec.Action == api.TestSpec_ACTION_PREEMPT && testSpec.Selection == api.TestSpec_SELECTION_BY_IDS:
						actionErr = client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
							_, err := sc.PreemptJobs(ctx, &api.JobPreemptRequest{
								Queue:    testSpec.GetQueue(),
								JobSetId: testSpec.GetJobSetId(),
								Reason:   testSpec.GetPreemptReason(),
								JobIds:   jobIds,
							})
							return errors.WithStack(err)
						})
					case testSpec.Action == api.TestSpec_ACTION_PREEMPT && testSpec.Selection == api.TestSpec_SELECTION_BY_SET:
						actionErr = client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
							_, err := sc.PreemptJobs(ctx, &api.JobPreemptRequest{
								Queue:    testSpec.GetQueue(),
								JobSetId: testSpec.GetJobSetId(),
								Reason:   testSpec.GetPreemptReason(),
							})
							return errors.WithStack(err)
						})
					default:
						return errors.Errorf("action/selection combination invalid or not yet implemented: %v/%v", testSpec.Action, testSpec.Selection)
					}
					if actionErr != nil {
						return actionErr
					}
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
