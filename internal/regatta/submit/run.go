package submit

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/regatta/load"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

const (
	// submitConcurrency caps SubmitJobs batches in flight across all queues, so hundreds of queues do not
	// open hundreds of simultaneous calls.
	submitConcurrency = 8
	progressInterval  = 10 * time.Second
)

// Run submits every queue's jobs on that queue's schedule, all measured from one common start, and
// returns once every step of every queue has submitted or one has failed (which cancels the rest). A
// continuous queue has no end: it submits until ctx is cancelled, and ending the run that way is a normal
// stop, not an error. Regatta is a load generator, not a job-completion tracker: it never polls job state,
// so it scales to submission batches far larger than would be practical to poll individually. Fake
// nodes/executors started for the run are left running - tear them down explicitly with `regatta
// teardown` once you're done collecting metrics.
func Run(ctx context.Context, apiConnectionDetails *client.ApiConnectionDetails, spec *Spec) error {
	return run(ctx, spec, func(queue, jobSetId, namespace string, jobs []JobItem) ([]string, error) {
		return submitItems(apiConnectionDetails, queue, jobSetId, namespace, jobs)
	})
}

type batchSubmitter func(queue, jobSetId, namespace string, jobs []JobItem) ([]string, error)

func run(ctx context.Context, spec *Spec, submit batchSubmitter) error {
	if spec.Continuous() {
		log.Infof("submitting %d jobs and then continuously, across %d queue(s), until stopped", spec.TotalJobs(), len(spec.Queues))
	} else {
		log.Infof("submitting %d jobs to %d queue(s)", spec.TotalJobs(), len(spec.Queues))
	}

	start := time.Now()
	var submitted atomic.Int64
	limiter := make(chan struct{}, submitConcurrency)

	group, groupCtx := errgroup.WithContext(ctx)
	for _, q := range spec.Queues {
		q := q
		group.Go(func() error {
			if q.Continuous != nil {
				return runContinuous(groupCtx, q, limiter, &submitted, submit)
			}
			return runQueue(groupCtx, q, start, limiter, &submitted, submit)
		})
	}

	done := make(chan struct{})
	defer close(done)
	go logProgress(done, &submitted, spec.TotalJobs(), spec.Continuous(), start)

	if err := group.Wait(); err != nil {
		// Stopping a run with a continuous queue is how it ends, so the cancellation is not a failure.
		if !(spec.Continuous() && ctx.Err() != nil && errors.Is(err, context.Canceled)) {
			return err
		}
	}
	log.Infof("submitted %d jobs to %d queue(s) in %s", submitted.Load(), len(spec.Queues), time.Since(start).Round(time.Millisecond))
	return nil
}

func runQueue(ctx context.Context, q QueueSpec, start time.Time, limiter chan struct{}, submitted *atomic.Int64, submit batchSubmitter) error {
	remaining := flattenJobItems(q.Jobs)
	for _, step := range q.Steps {
		if wait := time.Until(start.Add(step.Offset)); wait > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}
		batch := take(remaining, step.Count)
		remaining = remaining[len(batch):]

		if err := submitBatch(ctx, q, batch, limiter, submitted, submit); err != nil {
			return err
		}
	}
	return nil
}

// runContinuous returns nil when ctx is cancelled. Rates are fractional, so a Carry turns them into whole
// jobs without losing the remainder.
func runContinuous(ctx context.Context, q QueueSpec, limiter chan struct{}, submitted *atomic.Int64, submit batchSubmitter) error {
	rates := make([]float64, len(q.Continuous.Rates))
	for i, r := range q.Continuous.Rates {
		rates[i] = r.PerStep
	}
	carry := load.NewCarry(rates)

	ticker := time.NewTicker(q.Continuous.Step)
	defer ticker.Stop()
	for {
		var batch []JobItem
		for i, n := range carry.Next() {
			if n > 0 {
				batch = append(batch, JobItem{Spec: q.Continuous.Rates[i].Spec, Count: n})
			}
		}
		if len(batch) > 0 {
			if err := submitBatch(ctx, q, batch, limiter, submitted, submit); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func submitBatch(ctx context.Context, q QueueSpec, batch []JobItem, limiter chan struct{}, submitted *atomic.Int64, submit batchSubmitter) error {
	select {
	case limiter <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	jobIds, err := submit(q.Queue, q.JobSetId, q.Namespace, batch)
	<-limiter
	if err != nil {
		return fmt.Errorf("queue %q: submitting jobs: %w", q.Queue, err)
	}
	submitted.Add(int64(len(jobIds)))
	return nil
}

// logProgress logs one line in total, not one per queue, so a long schedule is not silent.
func logProgress(done <-chan struct{}, submitted *atomic.Int64, total int, continuous bool, start time.Time) {
	ticker := time.NewTicker(progressInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if continuous {
				log.Infof("submitted %d jobs so far (%s)", submitted.Load(), time.Since(start).Round(time.Second))
				continue
			}
			log.Infof("submitted %d/%d jobs (%s)", submitted.Load(), total, time.Since(start).Round(time.Second))
		}
	}
}

// submitItems submits count copies of each JobItem's PodSpec, in list order, returning every
// submitted job ID. The queue must already exist (see EnsureQueues).
func submitItems(apiConnectionDetails *client.ApiConnectionDetails, queue, jobSetId, namespace string, jobs []JobItem) ([]string, error) {
	if namespace == "" {
		namespace = "default"
	}

	var items []*api.JobSubmitRequestItem
	for _, job := range jobs {
		for i := 0; i < job.Count; i++ {
			items = append(items, &api.JobSubmitRequestItem{
				Namespace: namespace,
				PodSpec:   job.Spec,
			})
		}
	}

	var jobIds []string
	err := client.WithSubmitClient(apiConnectionDetails, func(submitClient api.SubmitClient) error {
		requests := client.CreateChunkedSubmitRequests(queue, jobSetId, items)
		for _, request := range requests {
			var response *api.JobSubmitResponse
			var err error
			for i := 0; i < QueueVisibilityRetries; i++ {
				response, err = client.SubmitJobs(submitClient, request)
				if err == nil || status.Code(err) != codes.PermissionDenied {
					break
				}
				time.Sleep(QueueVisibilityDelay)
			}
			if err != nil {
				return fmt.Errorf("submitting jobs: %w", err)
			}
			for _, item := range response.JobResponseItems {
				if item.Error != "" {
					return fmt.Errorf("job rejected: %s", item.Error)
				}
				jobIds = append(jobIds, item.JobId)
			}
		}
		return nil
	})
	return jobIds, err
}

// flattenJobItems expands each JobItem's Count into that many single-count JobItems, so take can
// slice across job-spec boundaries one job at a time.
func flattenJobItems(jobs []JobItem) []JobItem {
	var flat []JobItem
	for _, job := range jobs {
		for i := 0; i < job.Count; i++ {
			flat = append(flat, JobItem{Spec: job.Spec, Count: 1})
		}
	}
	return flat
}

// take returns up to n items from the front of flat, coalesced back into JobItems with Count>1
// where the same Spec repeats consecutively (keeps chunking/logging readable without changing
// submission order).
func take(flat []JobItem, n int) []JobItem {
	if n > len(flat) {
		n = len(flat)
	}
	var out []JobItem
	for _, item := range flat[:n] {
		if len(out) > 0 && out[len(out)-1].Spec == item.Spec {
			out[len(out)-1].Count++
			continue
		}
		out = append(out, JobItem{Spec: item.Spec, Count: 1})
	}
	return out
}
