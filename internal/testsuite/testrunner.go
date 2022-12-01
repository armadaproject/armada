package testsuite

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/G-Research/armada/internal/testsuite/eventbenchmark"
	"github.com/G-Research/armada/internal/testsuite/eventlogger"
	"github.com/G-Research/armada/internal/testsuite/eventsplitter"
	"github.com/G-Research/armada/internal/testsuite/eventwatcher"
	"github.com/G-Research/armada/internal/testsuite/submitter"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"golang.org/x/sync/errgroup"
)

type TestRunner struct {
	// Out is used to write output.
	Out io.Writer
	// Test spec
	// Events logger
	// Connection details of the Armada server to test.
	apiConnectionDetails *client.ApiConnectionDetails
	// Test to run.
	testSpec *api.TestSpec
	// Syste for periodic logging.
	eventLogger *eventlogger.EventLogger
	Report      *TestRunnerReport
}

type TestRunnerReport struct {
	TerminationReason string
	BenchmarkReport   *eventbenchmark.TestCaseBenchmarkReport
}

func (srv *TestRunner) Run(ctx context.Context) error {
	fmt.Fprintf(srv.Out, "starting test case "+testSpecHeader(srv.testSpec)+"\n")

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
	err := sbmtr.Run(ctx)
	if err != nil {
		return err
	}
	jobIds := sbmtr.JobIds()
	jobIdMap := make(map[string]bool)
	for _, jobId := range jobIds {
		jobIdMap[jobId] = false
	}

	// If configured, cancel the submitted jobs.
	if err := tryCancelJobs(ctx, srv.testSpec, srv.apiConnectionDetails, jobIds); err != nil {
		return err
	}

	// One channel for each system listening to events.
	benchmarkCh := make(chan *api.EventMessage)
	// logCh := make(chan *api.EventMessage)
	noActiveCh := make(chan *api.EventMessage)
	assertCh := make(chan *api.EventMessage)
	ingressCh := make(chan *api.EventMessage)

	// // Logger service.
	// eventLogger := eventlogger.New(logCh, logInterval)
	// eventLogger.Out = a.Out
	// g.Go(func() error { return eventLogger.Run(ctx) })

	// Goroutine forwarding API events on a channel.
	watcher := eventwatcher.New(srv.testSpec.Queue, srv.testSpec.JobSetId, srv.apiConnectionDetails)
	// TODO: Add as global config.
	watcher.BackoffExponential = time.Second
	watcher.MaxRetries = 6
	watcher.Out = srv.Out
	g.Go(func() error { return watcher.Run(ctx) })

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

	// Split the events into multiple channels, one for each downstream service.
	splitter := eventsplitter.New(
		watcher.C,
		[]chan *api.EventMessage{assertCh, ingressCh, noActiveCh, benchmarkCh, srv.eventLogger.In}...,
	)
	g.Go(func() error { return splitter.Run(ctx) })

	// Cancel the errgroup if there are no active jobs.
	g.Go(func() error { return eventwatcher.ErrorOnNoActiveJobs(ctx, noActiveCh, jobIdMap) })

	// Record time spent per job state.
	// Used to benchmark jobs.
	eventBenchmark := eventbenchmark.New(benchmarkCh)
	eventBenchmark.Out = srv.Out
	g.Go(func() error { return eventBenchmark.Run(ctx) })

	// Watch for ingress events and try to download from any ingresses found.
	g.Go(func() error { return eventwatcher.GetFromIngresses(ctx, ingressCh) })

	// Assert that we get the right events for each job.
	// Returns once we've received all events or when ctx is cancelled.
	// TODO: Delete the terminatedByJobId.
	g.Go(func() error {
		_, err := eventwatcher.AssertEvents(ctx, assertCh, jobIdMap, srv.testSpec.ExpectedEvents)
		return err
	})

	// Stop all services and wait for them to exit.
	// cancel()

	report := &TestRunnerReport{}
	if err := g.Wait(); err != nil {
		report.TerminationReason = err.Error()
	}

	// _, _ = fmt.Fprint(a.Out, "All job transitions:\n")
	// eventLogger.Log()

	// benchmark
	report.BenchmarkReport = eventBenchmark.NewTestCaseBenchmarkReport(srv.testSpec.GetName())
	// report.PrintStatistics(srv.Out)

	// TODO: Return reports somehow.
	// a.reports = append(a.reports, report)

	// Armada JobSet logs
	// TODO: Optionally get logs from failed jobs.
	// if testSpec.GetLogs && executorClustersDefined {
	// 	jobLogger.PrintLogs()
	// }

	// Cancel the job set
	err = client.WithSubmitClient(srv.apiConnectionDetails, func(sc api.SubmitClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := sc.CancelJobSet(ctx, &api.JobSetCancelRequest{
			JobSetId: srv.testSpec.JobSetId,
			Queue:    srv.testSpec.Queue,
		})
		return err
	})
	if err != nil {
		fmt.Fprintf(srv.Out, "failed to cancel job set: %s", err)
	}

	return nil
}
