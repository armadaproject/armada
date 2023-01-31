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
	report := &TestCaseReport{
		Out:      &bytes.Buffer{},
		Start:    time.Now(),
		TestSpec: srv.testSpec,
	}
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
	if srv.testSpec.Timeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, srv.testSpec.Timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	// Setup an errgroup that cancels on any job failing or there being no active jobs.
	g, ctx := errgroup.WithContext(ctx)

	// Submit jobs. All jobs must be submitted before proceeding since we need the job ids.
	sbmtr := submitter.NewSubmitterFromTestSpec(srv.apiConnectionDetails, srv.testSpec)
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

	// Duplicate events across all downstream services.
	splitter := eventsplitter.New(
		watcher.C,
		[]chan *api.EventMessage{assertCh, ingressCh, noActiveCh, benchmarkCh, srv.eventLogger.In}...,
	)
	g.Go(func() error { return splitter.Run(ctx) })

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
	req := &api.JobCancelRequest{
		Queue:    testSpec.GetQueue(),
		JobSetId: testSpec.GetJobSetId(),
	}
	switch {
	case testSpec.Cancel == api.TestSpec_BY_ID:
		return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
			for _, jobId := range jobIds {
				req.JobId = jobId
				_, err := sc.CancelJobs(ctx, req)
				if err != nil {
					return errors.WithStack(err)
				}
			}
			return nil
		})
	case testSpec.Cancel == api.TestSpec_BY_SET:
		return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
			_, err := sc.CancelJobs(ctx, req)
			if err != nil {
				return errors.WithStack(err)
			}
			return nil
		})
	case testSpec.Cancel == api.TestSpec_BY_IDS:
		return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
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
