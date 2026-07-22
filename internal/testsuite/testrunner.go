package testsuite

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/pkg/errors"
	"github.com/renstrom/shortuuid"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

	var cancel context.CancelFunc
	timeout := protoutil.ToStdDuration(srv.testSpec.Timeout)
	if timeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	// Phase 1: Queue setup (create queue(s) if configured).
	queueNames, err := runQueueSetup(ctx, srv.testSpec, srv.apiConnectionDetails, out)
	if err != nil {
		return err
	}

	// Phase 1b: Queue update (update queue priority factor if configured).
	if err = runQueueUpdate(ctx, queueNames, srv.testSpec, srv.apiConnectionDetails, out); err != nil {
		return err
	}

	// Phase 4 (deferred): Queue teardown — registered here, executes last after all phases complete.
	defer func() {
		if teardownErr := runQueueTeardown(queueNames, srv.testSpec, srv.apiConnectionDetails, out); teardownErr != nil {
			fmt.Fprintf(out, "warning: queue teardown failed: %s\n", teardownErr)
			if err == nil {
				err = teardownErr
			}
		}
	}()

	// Phase 2: Job submission and event watching (skip for pure queue tests).
	if len(srv.testSpec.Jobs) > 0 || srv.testSpec.NumBatches > 0 {
		g, ctx := errgroup.WithContext(ctx)

		sbmtr := submitter.NewSubmitterFromTestSpec(srv.apiConnectionDetails, srv.testSpec, out)
		if err = sbmtr.Run(ctx); err != nil {
			return err
		}
		jobIds := sbmtr.JobIds()
		jobIdMap := make(map[string]bool)
		for _, jobId := range jobIds {
			jobIdMap[jobId] = false
		}

		defer func() {
			cancelErr := client.WithSubmitClient(srv.apiConnectionDetails, func(sc api.SubmitClient) error {
				cancelCtx, cancelCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancelCancel()
				_, cancelJobSetErr := sc.CancelJobSet(cancelCtx, &api.JobSetCancelRequest{
					JobSetId: srv.testSpec.JobSetId,
					Queue:    srv.testSpec.Queue,
				})
				return cancelJobSetErr
			})
			if cancelErr != nil {
				fmt.Fprintf(out, "failed to cancel job set %s: %s\n", srv.testSpec.JobSetId, cancelErr)
			}
		}()

		if err = tryCancelJobs(ctx, srv.testSpec, srv.apiConnectionDetails, jobIds); err != nil {
			return err
		}

		benchmarkCh := make(chan *api.EventMessage)
		noActiveCh := make(chan *api.EventMessage)
		assertCh := make(chan *api.EventMessage)
		ingressCh := make(chan *api.EventMessage)

		watcher := eventwatcher.New(srv.testSpec.Queue, srv.testSpec.JobSetId, srv.apiConnectionDetails)
		watcher.Out = out
		g.Go(func() error { return watcher.Run(ctx) })

		eventChannels := []chan *api.EventMessage{assertCh, ingressCh, noActiveCh, benchmarkCh, srv.eventLogger.In}

		// Add preempt channel if preemption is configured.
		var preemptCh chan *api.EventMessage
		if srv.testSpec.Action == api.TestSpec_ACTION_PREEMPT {
			preemptCh = make(chan *api.EventMessage)
			eventChannels = append(eventChannels, preemptCh)
		}

		// Add reprioritize channel if reprioritization is configured.
		var reprioritizeCh chan *api.EventMessage
		if srv.testSpec.Action == api.TestSpec_ACTION_REPRIORITIZE {
			reprioritizeCh = make(chan *api.EventMessage)
			eventChannels = append(eventChannels, reprioritizeCh)
		}

		splitter := eventsplitter.New(watcher.C, eventChannels...)
		g.Go(func() error { return splitter.Run(ctx) })

		// If configured, preempt jobs once they are running.
		// Used to test job preemption.
		if srv.testSpec.Action == api.TestSpec_ACTION_PREEMPT {
			g.Go(func() error {
				return preemptJobsWhenRunning(ctx, preemptCh, srv.testSpec, srv.apiConnectionDetails, jobIds)
			})
		}

		// If configured, reprioritize jobs once they are running.
		// Used to test job reprioritization. Waiting for the jobs to be running
		// (rather than sleeping a fixed interval) ensures the reprioritize request
		// is not handled before its jobs exist in the scheduler, which would produce
		// no reprioritized events.
		if srv.testSpec.Action == api.TestSpec_ACTION_REPRIORITIZE {
			g.Go(func() error {
				return reprioritizeJobsWhenRunning(ctx, reprioritizeCh, srv.testSpec, srv.apiConnectionDetails, jobIds)
			})
		}

		// If configured, reprioritize jobs once they are running.
		// Used to test job reprioritization. Waiting for the jobs to be running
		// (rather than sleeping a fixed interval) ensures the reprioritize request
		// is not handled before its jobs exist in the scheduler, which would produce
		// no reprioritized events.
		if srv.testSpec.Action == api.TestSpec_ACTION_REPRIORITIZE {
			g.Go(func() error {
				return reprioritizeJobsWhenRunning(ctx, reprioritizeCh, srv.testSpec, srv.apiConnectionDetails, jobIds)
			})
		}

		g.Go(func() error { return eventwatcher.ErrorOnNoActiveJobs(ctx, noActiveCh, maps.Clone(jobIdMap)) })

		eventBenchmark := eventbenchmark.New(benchmarkCh)
		eventBenchmark.Out = out
		g.Go(func() error { return eventBenchmark.Run(ctx) })
		defer func() {
			report.BenchmarkReport = eventBenchmark.NewTestCaseBenchmarkReport(srv.testSpec.GetName())
		}()

		g.Go(func() error { return eventwatcher.GetFromIngresses(ctx, ingressCh) })

		if err = eventwatcher.AssertEvents(ctx, assertCh, maps.Clone(jobIdMap), srv.testSpec.ExpectedEvents); err != nil {
			cancel()
			groupErr := g.Wait()
			if groupErr != nil {
				return errors.Errorf("%s: %s", err, groupErr)
			}
			return err
		}
	}

	// Phase 3: Queue assertions (after jobs finish, or for pure queue tests).
	if err = runQueueAssertions(ctx, srv.testSpec, srv.apiConnectionDetails, out); err != nil {
		return err
	}

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

// tryReprioritizeJobs reprioritizes submitted jobs if reprioritization is configured.
func tryReprioritizeJobs(ctx context.Context, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, jobIds []string) error {
	if testSpec.Action != api.TestSpec_ACTION_REPRIORITIZE {
		return nil
	}

	req := &api.JobReprioritizeRequest{
		Queue:       testSpec.GetQueue(),
		JobSetId:    testSpec.GetJobSetId(),
		NewPriority: testSpec.GetNewPriority(),
	}
	switch {
	case testSpec.Selection == api.TestSpec_SELECTION_BY_ID:
		return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
			for _, jobId := range jobIds {
				req.JobIds = []string{jobId}
				_, err := sc.ReprioritizeJobs(ctx, req)
				if err != nil {
					return errors.WithStack(err)
				}
			}
			return nil
		})
	case testSpec.Selection == api.TestSpec_SELECTION_BY_SET:
		return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
			_, err := sc.ReprioritizeJobs(ctx, req)
			if err != nil {
				return errors.WithStack(err)
			}
			return nil
		})
	case testSpec.Selection == api.TestSpec_SELECTION_BY_IDS:
		return client.WithSubmitClient(conn, func(sc api.SubmitClient) error {
			req.JobIds = jobIds
			_, err := sc.ReprioritizeJobs(ctx, req)
			if err != nil {
				return errors.WithStack(err)
			}
			return nil
		})
	}
	return nil
}

// reprioritizeJobsWhenRunning waits for all jobs to be running, then
// reprioritizes them. Waiting for the Running event (rather than sleeping a fixed
// interval or reacting to an early Queued event) guarantees each job is present in
// the scheduler's job db before the reprioritize request is handled. Otherwise the
// fire-once reprioritization can run before a job exists in the scheduler, so no
// reprioritized event is produced and the test times out intermittently.
func reprioritizeJobsWhenRunning(ctx context.Context, eventCh chan *api.EventMessage, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, jobIds []string) error {
	// Only this runner's submitted jobs should count toward the readiness gate.
	// The job set may be shared with concurrently-running test cases, so a Running
	// event from another case must not satisfy the gate and fire the reprioritize
	// request before this case's own jobs reach the scheduler.
	submittedJobs := make(map[string]bool, len(jobIds))
	for _, jobId := range jobIds {
		submittedJobs[jobId] = true
	}

	runningJobs := make(map[string]bool)

	// Wait for all jobs to be running before reprioritizing.
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-eventCh:
			if e := msg.GetRunning(); e != nil && submittedJobs[e.JobId] {
				runningJobs[e.JobId] = true

				// Once all jobs are running, reprioritize them.
				if len(runningJobs) == len(jobIds) {
					if err := tryReprioritizeJobs(ctx, testSpec, conn, jobIds); err != nil {
						return err
					}
					// Continue consuming events to avoid blocking the splitter.
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

// applyQueueRandomSuffix appends a random suffix to testSpec.Queue if configured.
func applyQueueRandomSuffix(testSpec *api.TestSpec) {
	if testSpec.GetQueueConfig().GetSetup().GetRandomSuffix() {
		testSpec.Queue = testSpec.Queue + "-" + shortuuid.New()
	}
}

// runQueueSetup creates the queue(s) if configured, and returns the names of the queues created.
// Mirrors submitter.Submitter.Run's batching structure: batch_size queues are created per round,
// for num_batches rounds (defaults to 1 if unset), waiting interval between rounds.
func runQueueSetup(ctx context.Context, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, out io.Writer) ([]string, error) {
	setup := testSpec.GetQueueConfig().GetSetup()
	if setup == nil || !setup.Create {
		return nil, nil
	}
	batchSize := setup.GetBatchSize()
	if batchSize == 0 {
		batchSize = 1
	}
	numBatches := setup.GetNumBatches()
	if numBatches == 0 {
		numBatches = 1
	}
	interval := protoutil.ToStdDuration(setup.GetInterval())
	queueSpecs := setup.GetQueueSpecs()

	var queueNames []string
	err := client.WithQueueServiceClient(conn, func(qsc api.QueueServiceClient) error {
		// Create a closed ticker channel; receiving on tickerCh returns immediately.
		C := make(chan time.Time)
		close(C)
		tickerCh := (<-chan time.Time)(C)

		// If an interval is provided, replace tickerCh with one that generates ticks periodically.
		if interval != 0 {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			tickerCh = ticker.C
		}

		var numBatchesCreated uint32
		for numBatchesCreated < numBatches {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-tickerCh:
				batchQueues := queuesForBatch(testSpec.Queue, queueSpecs, numBatchesCreated, numBatches, batchSize, setup.RandomSuffix)
				if err := createQueueBatch(ctx, qsc, batchQueues, out); err != nil {
					return err
				}
				fmt.Fprintf(out, "created %d queue(s)\n", len(batchQueues))
				for _, queue := range batchQueues {
					queueNames = append(queueNames, queue.Name)
				}
				numBatchesCreated++
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return queueNames, nil
}

// queuesForBatch builds the queues to create for a single batch. If queueSpecs is non-empty, one
// copy of each entry is created per queue slot in the batch (batchSize slots); otherwise a single
// queue named after baseName (with default priority factor 1.0) is created per slot.
func queuesForBatch(baseName string, queueSpecs []*api.Queue, batchIndex uint32, numBatches uint32, batchSize uint32, randomSuffix bool) []*api.Queue {
	totalCopies := numBatches * batchSize
	var queues []*api.Queue
	for i := uint32(0); i < batchSize; i++ {
		index := batchIndex*batchSize + i
		if len(queueSpecs) == 0 {
			queues = append(queues, &api.Queue{
				Name:           queueNameForIndex(baseName, index, numBatches, batchSize),
				PriorityFactor: 1.0,
			})
			continue
		}
		for _, spec := range queueSpecs {
			queues = append(queues, queueFromSpecForIndex(spec, index, totalCopies, randomSuffix))
		}
	}
	return queues
}

// queueFromSpecForIndex clones spec, giving the copy a unique name: a random suffix if
// randomSuffix is set (so the same spec can be reused to create many batches of queues with
// identical properties), otherwise "-<index>" if more than one copy will be created.
func queueFromSpecForIndex(spec *api.Queue, index uint32, totalCopies uint32, randomSuffix bool) *api.Queue {
	clone := proto.Clone(spec).(*api.Queue)
	if clone.PriorityFactor == 0 {
		clone.PriorityFactor = 1.0
	}
	switch {
	case randomSuffix:
		clone.Name = clone.Name + "-" + shortuuid.New()
	case totalCopies > 1:
		clone.Name = fmt.Sprintf("%s-%d", clone.Name, index)
	}
	return clone
}

// queueNameForIndex returns the name for the queue at the given index: baseName exactly if
// only a single queue will ever be created (numBatches == 1 && batchSize == 1), or
// "<baseName>-<index>" otherwise.
func queueNameForIndex(baseName string, index uint32, numBatches uint32, batchSize uint32) string {
	if numBatches == 1 && batchSize == 1 {
		return baseName
	}
	return fmt.Sprintf("%s-%d", baseName, index)
}

// createQueueBatch creates the given queues, using the batched CreateQueues endpoint
// (POST /v1/batched/create_queues) when there is more than one queue to create so that
// endpoint is exercised. Falls back to individual CreateQueue calls for a single queue.
func createQueueBatch(ctx context.Context, qsc api.QueueServiceClient, queues []*api.Queue, out io.Writer) error {
	if len(queues) == 1 {
		return createOneQueue(ctx, qsc, queues[0], out)
	}
	createCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := qsc.CreateQueues(createCtx, &api.QueueList{Queues: queues})
	if err != nil {
		return errors.Wrap(err, "CreateQueues (batched) failed")
	}
	for _, failed := range resp.GetFailedQueues() {
		if strings.Contains(failed.GetError(), codes.AlreadyExists.String()) {
			fmt.Fprintf(out, "queue %s already exists, continuing\n", failed.GetQueue().GetName())
			continue
		}
		return fmt.Errorf("failed to create queue %s: %s", failed.GetQueue().GetName(), failed.GetError())
	}
	return nil
}

// createOneQueue creates a single queue.
func createOneQueue(ctx context.Context, qsc api.QueueServiceClient, queue *api.Queue, out io.Writer) error {
	createCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := qsc.CreateQueue(createCtx, queue)
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.AlreadyExists {
			fmt.Fprintf(out, "queue %s already exists, continuing\n", queue.Name)
			return nil
		}
		return errors.Wrapf(err, "failed to create queue %s", queue.Name)
	}
	return nil
}

// runQueueUpdate applies the configured update to the queue(s), if configured.
func runQueueUpdate(ctx context.Context, queueNames []string, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, out io.Writer) error {
	update := testSpec.GetQueueConfig().GetUpdate()
	if update == nil {
		return nil
	}
	if len(queueNames) == 0 {
		queueNames = []string{testSpec.Queue}
	}
	return client.WithQueueServiceClient(conn, func(qsc api.QueueServiceClient) error {
		queues := make([]*api.Queue, len(queueNames))
		for i, queueName := range queueNames {
			queue := proto.Clone(update).(*api.Queue)
			queue.Name = queueName
			if queue.PriorityFactor == 0 {
				queue.PriorityFactor = 1.0
			}
			queues[i] = queue
		}
		if len(queues) == 1 {
			updateCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			_, err := qsc.UpdateQueue(updateCtx, queues[0])
			cancel()
			if err != nil {
				return errors.Wrapf(err, "failed to update queue %s", queues[0].Name)
			}
		} else {
			// Use the batched UpdateQueues endpoint (PUT /v1/batched/update_queues) to exercise it.
			updateCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			resp, err := qsc.UpdateQueues(updateCtx, &api.QueueList{Queues: queues})
			cancel()
			if err != nil {
				return errors.Wrap(err, "UpdateQueues (batched) failed")
			}
			if failed := resp.GetFailedQueues(); len(failed) > 0 {
				return fmt.Errorf("failed to update queue %s: %s", failed[0].GetQueue().GetName(), failed[0].GetError())
			}
		}
		fmt.Fprintf(out, "updated %d queue(s)\n", len(queueNames))
		return nil
	})
}

// runQueueTeardown deletes the queue(s), unless queueConfig.teardown.skipDelete is set.
func runQueueTeardown(queueNames []string, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, out io.Writer) error {
	config := testSpec.GetQueueConfig()
	if config == nil {
		return nil
	}
	teardown := config.GetTeardown()
	if teardown.GetSkipDelete() {
		return nil
	}
	if len(queueNames) == 0 {
		queueNames = []string{testSpec.Queue}
	}
	return client.WithQueueServiceClient(conn, func(qsc api.QueueServiceClient) error {
		var deleted []string
		for _, queueName := range queueNames {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err := qsc.DeleteQueue(ctx, &api.QueueDeleteRequest{Name: queueName})
			cancel()
			if err != nil {
				if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
					if teardown.GetExpectNotFound() {
						fmt.Fprintf(out, "correctly received NOT_FOUND when deleting non-existent queue %s\n", queueName)
						continue
					}
					fmt.Fprintf(out, "queue %s already deleted, skipping teardown\n", queueName)
					continue // already gone; that's fine
				}
				return errors.Wrapf(err, "failed to delete queue %s", queueName)
			}
			if teardown.GetExpectNotFound() {
				return fmt.Errorf("expected NOT_FOUND when deleting queue %s but delete succeeded", queueName)
			}
			deleted = append(deleted, queueName)
		}
		if len(deleted) > 0 {
			fmt.Fprintf(out, "deleted %d queue(s)\n", len(deleted))
		}
		if assertDeleted(testSpec) {
			for _, queueName := range deleted {
				getCtx, getCancel := context.WithTimeout(context.Background(), 10*time.Second)
				_, err := qsc.GetQueue(getCtx, &api.QueueGetRequest{Name: queueName})
				getCancel()
				if err == nil {
					return fmt.Errorf("expected queue %s to be deleted, but GetQueue succeeded", queueName)
				}
				if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
					return errors.Wrapf(err, "expected NOT_FOUND asserting queue %s was deleted, got", queueName)
				}
			}
			fmt.Fprintf(out, "asserted %d queue(s) were deleted\n", len(deleted))
		}
		return nil
	})
}

// assertDeleted returns true if any configured assertion requests that deleted queue(s)
// be verified as NOT_FOUND via GetQueue.
func assertDeleted(testSpec *api.TestSpec) bool {
	for _, assertion := range testSpec.GetQueueConfig().GetAssertions() {
		if assertion.GetDeleted() {
			return true
		}
	}
	return false
}

// runQueueAssertions checks queue state assertions.
func runQueueAssertions(ctx context.Context, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, out io.Writer) error {
	assertions := testSpec.GetQueueConfig().GetAssertions()
	if len(assertions) == 0 {
		return nil
	}
	for _, assertion := range assertions {
		if pool := assertion.ActiveInPool; pool != "" {
			if err := client.WithJobsClient(conn, func(jc api.JobsClient) error {
				reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				defer cancel()
				resp, err := jc.GetActiveQueues(reqCtx, &api.GetActiveQueuesRequest{})
				if err != nil {
					return errors.Wrap(err, "GetActiveQueues failed")
				}
				activeQueues, ok := resp.ActiveQueuesByPool[pool]
				if !ok {
					return fmt.Errorf("pool %q not found in GetActiveQueues response", pool)
				}
				for _, q := range activeQueues.Queues {
					if q == testSpec.Queue {
						fmt.Fprintf(out, "asserted queue %s is active in pool %s\n", testSpec.Queue, pool)
						return nil
					}
				}
				return fmt.Errorf("queue %q not found in active queues for pool %q", testSpec.Queue, pool)
			}); err != nil {
				return err
			}
		}
		if pool := assertion.NotActiveInPool; pool != "" {
			if err := client.WithJobsClient(conn, func(jc api.JobsClient) error {
				reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				defer cancel()
				resp, err := jc.GetActiveQueues(reqCtx, &api.GetActiveQueuesRequest{})
				if err != nil {
					return errors.Wrap(err, "GetActiveQueues failed")
				}
				activeQueues, ok := resp.ActiveQueuesByPool[pool]
				if ok {
					for _, q := range activeQueues.Queues {
						if q == testSpec.Queue {
							return fmt.Errorf("queue %q unexpectedly found in active queues for pool %q", testSpec.Queue, pool)
						}
					}
				}
				fmt.Fprintf(out, "asserted queue %s is NOT active in pool %s\n", testSpec.Queue, pool)
				return nil
			}); err != nil {
				return err
			}
		}
		if assertion.AppearsInStream {
			if err := client.WithQueueServiceClient(conn, func(qsc api.QueueServiceClient) error {
				streamCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				defer cancel()
				stream, err := qsc.GetQueues(streamCtx, &api.StreamingQueueGetRequest{})
				if err != nil {
					return errors.Wrap(err, "GetQueues stream failed to open")
				}
				for {
					msg, err := stream.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						return errors.Wrap(err, "GetQueues stream recv error")
					}
					if q := msg.GetQueue(); q != nil && q.Name == testSpec.Queue {
						fmt.Fprintf(out, "asserted queue %s appears in GetQueues stream\n", testSpec.Queue)
						return nil
					}
				}
				return fmt.Errorf("queue %q not found in GetQueues stream", testSpec.Queue)
			}); err != nil {
				return err
			}
		}
		if expected := assertion.Matches; expected != nil {
			if err := client.WithQueueServiceClient(conn, func(qsc api.QueueServiceClient) error {
				getCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				defer cancel()
				actual, err := qsc.GetQueue(getCtx, &api.QueueGetRequest{Name: testSpec.Queue})
				if err != nil {
					return errors.Wrap(err, "GetQueue failed")
				}
				want := proto.Clone(expected).(*api.Queue)
				want.Name = actual.Name
				if !proto.Equal(want, actual) {
					return fmt.Errorf("queue %q properties did not match: got %+v, want %+v", testSpec.Queue, actual, want)
				}
				fmt.Fprintf(out, "asserted queue %s properties match expected\n", testSpec.Queue)
				return nil
			}); err != nil {
				return err
			}
		}
	}
	return nil
}
