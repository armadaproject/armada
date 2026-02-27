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

	// If configured, cancel the submitted jobs immediately.
	// Used to test job cancellation.
	if err = tryCancelJobs(ctx, srv.testSpec, srv.apiConnectionDetails, jobIds); err != nil {
		return err
	}

	// One channel for each system listening to events.
	benchmarkCh := make(chan *api.EventMessage)
	noActiveCh := make(chan *api.EventMessage)
	assertCh := make(chan *api.EventMessage)
	ingressCh := make(chan *api.EventMessage)

	// Goroutine forwarding API events on a channel.
	watcher := eventwatcher.New(srv.testSpec.Queue, srv.testSpec.JobSetId, srv.apiConnectionDetails)
	watcher.Out = out
	g.Go(func() error { return watcher.Run(ctx) })

	// TODO: Get job logs.
	// jobLogger, err := a.createJobLogger(testSpec)
	// if err != nil {
	// 	return errors.WithMessage(err, "error creating job logger")
	// }
	// executorClustersDefined := len(a.Params.ApiConnectionDetails.ExecutorClusters) > 0
	// if testSpec.GetLogs {
	// 	if executorClustersDefined {
	// 		g.Go(func() error { return jobLogger.Run(ctx) })
	// 	} else {
	// 		_, _ = fmt.Fprintf(
	// 			a.Out,
	// 			"cannot get logs for test %s, no executor clusters specified in executorClusters config\n",
	// 			testSpec.Name,
	// 		)
	// 	}
	// }

	// Build list of event channels based on test configuration.
	eventChannels := []chan *api.EventMessage{assertCh, ingressCh, noActiveCh, benchmarkCh, srv.eventLogger.In}

	// Add preempt channel if preemption is configured.
	var preemptCh chan *api.EventMessage
	if srv.testSpec.Action == api.TestSpec_ACTION_PREEMPT {
		preemptCh = make(chan *api.EventMessage)
		eventChannels = append(eventChannels, preemptCh)
	}

	// Duplicate events across all downstream services.
	splitter := eventsplitter.New(
		watcher.C,
		eventChannels...,
	)
	g.Go(func() error { return splitter.Run(ctx) })

	// If configured, preempt jobs once they are running.
	// Used to test job preemption.
	if srv.testSpec.Action == api.TestSpec_ACTION_PREEMPT {
		g.Go(func() error {
			return preemptJobsWhenRunning(ctx, preemptCh, srv.testSpec, srv.apiConnectionDetails, jobIds)
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

	// Armada JobSet logs
	// TODO: Optionally get logs from failed jobs.
	// if testSpec.GetLogs && executorClustersDefined {
	// 	jobLogger.PrintLogs()
	// }

	return nil
}

// tryCancelJobs cancels submitted jobs if cancellation is configured.
func tryCancelJobs(ctx context.Context, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, jobIds []string) error {
	if testSpec.Action != api.TestSpec_ACTION_CANCEL {
		return nil
	}

	req := &api.JobCancelRequest{
		Queue:    testSpec.GetQueue(),
		JobSetId: testSpec.GetJobSetId(),
	}
	switch {
	case testSpec.Selection == api.TestSpec_SELECTION_BY_ID:
		return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
			time.Sleep(3 * time.Second)
			for _, jobId := range jobIds {
				req.JobId = jobId
				_, err := sc.CancelJobs(ctx, req)
				if err != nil {
					return errors.WithStack(err)
				}
			}
			return nil
		})
	case testSpec.Selection == api.TestSpec_SELECTION_BY_SET:
		return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
			time.Sleep(3 * time.Second)
			_, err := sc.CancelJobs(ctx, req)
			if err != nil {
				return errors.WithStack(err)
			}
			return nil
		})
	case testSpec.Selection == api.TestSpec_SELECTION_BY_IDS:
		return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
			time.Sleep(3 * time.Second)
			req.JobIds = jobIds
			_, err := sc.CancelJobs(ctx, req)
			if err != nil {
				return errors.WithStack(err)
			}
			return nil
		})
	}
	return nil
}

// preemptJobsWhenRunning waits for jobs to be running, then preempts them.
func preemptJobsWhenRunning(ctx context.Context, eventCh chan *api.EventMessage, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, jobIds []string) error {
	runningJobs := make(map[string]bool)

	// Wait for all jobs to be running
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-eventCh:
			if e := msg.GetRunning(); e != nil {
				runningJobs[e.JobId] = true

				// Once all jobs are running, preempt them
				if len(runningJobs) == len(jobIds) {
					time.Sleep(1 * time.Second) // Brief delay to ensure job is fully running
					_ = tryPreemptJobs(ctx, testSpec, conn, jobIds)
					// Continue consuming events but don't preempt again
					for {
						select {
						case <-ctx.Done():
							return nil
						case <-eventCh:
							// Keep consuming to avoid blocking the splitter
						}
					}
				}
			}
		}
	}
}

// tryPreemptJobs preempts submitted jobs if preemption is configured.
func tryPreemptJobs(ctx context.Context, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, jobIds []string) error {
	req := &api.JobPreemptRequest{
		Queue:    testSpec.GetQueue(),
		JobSetId: testSpec.GetJobSetId(),
		Reason:   testSpec.GetPreemptReason(),
	}
	switch {
	case testSpec.Selection == api.TestSpec_SELECTION_BY_ID:
		return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
			for _, jobId := range jobIds {
				req.JobIds = []string{jobId}
				_, err := sc.PreemptJobs(ctx, req)
				if err != nil {
					return errors.WithStack(err)
				}
			}
			return nil
		})
	case testSpec.Selection == api.TestSpec_SELECTION_BY_SET:
		return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
			_, err := sc.PreemptJobs(ctx, req)
			if err != nil {
				return errors.WithStack(err)
			}
			return nil
		})
	case testSpec.Selection == api.TestSpec_SELECTION_BY_IDS:
		return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
			req.JobIds = jobIds
			_, err := sc.PreemptJobs(ctx, req)
			if err != nil {
				return errors.WithStack(err)
			}
			return nil
		})
	}
	return nil
}
