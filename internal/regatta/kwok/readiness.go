package kwok

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/regatta/submit"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/api/schedulerobjects"
	"github.com/armadaproject/armada/pkg/client"
)

const (
	// canaryJobSetPrefix starts the job set id every readiness check uses. Canary jobs get a job set of
	// their own - never the load's - so their events can't be confused with the load's.
	canaryJobSetPrefix = "regatta-readiness"

	// schedulerReportTimeout bounds the best-effort scheduler report fetched for the error
	// message when an attempt times out.
	schedulerReportTimeout = 5 * time.Second
)

// ReadinessConfig controls WaitUntilSchedulable's retry behaviour.
type ReadinessConfig struct {
	Retries      int           // number of canary-job attempts before giving up
	InitialDelay time.Duration // how long the first attempt waits for the canary to run; doubles each retry
	// SelectTarget makes the canary also select on this target's armadaproject.io/regatta-target
	// node label, so it can only land on this target's fake nodes. Needed when several targets
	// share one Armada; it requires the executors to report that label. Without it the canary
	// selects on the KWOK annotation alone.
	SelectTarget bool
}

// WaitUntilSchedulable submits a throwaway canary job that only tolerates the KWOK fake-node
// taint, and blocks until Armada reports the job running - i.e. until the executor has reported
// the fake nodes' capacity to the scheduler, which has leased the canary onto one of them, and
// the executor has started its pod. K8s "Ready" alone doesn't mean the scheduler knows about a
// node yet, since the executor only reports on its own poll interval.
//
// The signal is the job's own lifecycle events on Armada's event API (the stream `armadactl
// watch` uses), so it needs no access to the target's Kubernetes API and works the same for a
// kind cluster and an external one. Each attempt waits up to the current delay for the canary to
// reach Running, then cancels it and tries again with the delay doubled, since there's no direct
// way to ask the scheduler "do you know about this node yet". A timed-out attempt's error carries
// the scheduler's own explanation of why the canary is still queued, when it has one.
func WaitUntilSchedulable(ctx context.Context, apiConnectionDetails *client.ApiConnectionDetails, cfg ReadinessConfig, targetName, queue string) error {
	jobSetId := fmt.Sprintf("%s-%s-%s-%d", canaryJobSetPrefix, targetName, queue, time.Now().Unix())
	delay := cfg.InitialDelay
	checkStart := time.Now()
	log.Infof("target %q: readiness check: canary in queue %q, up to %d attempts, first wait %s (selects target label: %t)",
		targetName, queue, cfg.Retries, delay, cfg.SelectTarget)
	var last attemptFailure
	for attempt := 1; attempt <= cfg.Retries; attempt++ {
		attemptStart := time.Now()
		jobId, err := submitCanaryJob(apiConnectionDetails, queue, jobSetId, targetName, cfg.SelectTarget)
		if err != nil {
			last = attemptFailure{brief: "canary not submitted: " + err.Error()}
			last.detail = last.brief
		} else {
			log.Infof("target %q: readiness attempt %d/%d: canary %s submitted, waiting up to %s", targetName, attempt, cfg.Retries, jobId, delay)
			var outcome canaryOutcome
			err = client.WithEventClient(apiConnectionDetails, func(eventClient api.EventClient) error {
				var watchErr error
				outcome, watchErr = awaitCanaryRunning(ctx, eventClient, queue, jobSetId, jobId, delay)
				return watchErr
			})
			var view schedulerView
			if err == nil && !outcome.running && outcome.failure == "" {
				// Fetched before the cancel below, while the scheduler still remembers the job.
				view = fetchSchedulerView(apiConnectionDetails, jobId)
			}
			cancelCanaryJob(apiConnectionDetails, queue, jobSetId, jobId)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			switch {
			case err != nil:
				last = attemptFailure{brief: fmt.Sprintf("canary job %s: %s", jobId, err)}
				last.detail = last.brief
			case outcome.running:
				log.Infof("target %q: readiness check passed: canary %s running %s after submit (attempt %d/%d, %s since the check started)",
					targetName, jobId, time.Since(attemptStart).Round(time.Millisecond), attempt, cfg.Retries, time.Since(checkStart).Round(time.Millisecond))
				return nil
			case outcome.failure != "":
				last = attemptFailure{brief: fmt.Sprintf("canary job %s %s", jobId, outcome.failure)}
				last.detail = last.brief
			default:
				last = timedOutFailure(jobId, delay, outcome.last, view)
			}
		}

		log.Warnf("target %q: readiness attempt %d/%d failed: %s", targetName, attempt, cfg.Retries, last.brief)
		// An attempt can end in well under its delay: the scheduler's submit check fails a canary
		// that fits no node it knows of within about a second, and that view of the nodes only
		// refreshes every scheduling.executorUpdateFrequency (60s by default). Retrying at once
		// would spend every attempt inside that window, so pace retries by the backoff instead.
		if attempt < cfg.Retries {
			if err := waitRemaining(ctx, attemptStart, delay); err != nil {
				return err
			}
		}
		delay *= 2
	}
	return fmt.Errorf("fake nodes never became schedulable after %d attempts: %s", cfg.Retries, last.detail)
}

// attemptFailure is why one canary attempt did not get the canary running: brief is the short
// text for the per-attempt log line, detail the longer text for the error returned once every
// attempt has failed.
type attemptFailure struct {
	brief, detail string
}

// timedOutFailure describes an attempt whose canary was still not running when its wait ended. The
// log line only says how many nodes the scheduler had (falling back to the last event when it had
// no report); the final error adds that event and why the scheduler excluded nodes.
func timedOutFailure(jobId string, wait time.Duration, lastEvent string, view schedulerView) attemptFailure {
	f := attemptFailure{brief: "canary job " + jobId}
	if view.nodes != "" {
		f.brief += ", Number of nodes in cluster: " + view.nodes
	} else {
		f.brief += ", last event " + lastEventOrNone(lastEvent)
	}
	f.detail = fmt.Sprintf("canary job %s not running within %s, last event %s", jobId, wait, lastEventOrNone(lastEvent))
	if view.nodes != "" {
		f.detail += "; Number of nodes in cluster: " + view.nodes
	}
	if len(view.excluded) > 0 {
		f.detail += "; excluded nodes: " + strings.Join(view.excluded, "; ")
	}
	return f
}

// waitRemaining blocks until budget has elapsed since start, or ctx ends (returning its error).
// It returns at once if the budget has already been used up.
func waitRemaining(ctx context.Context, start time.Time, budget time.Duration) error {
	remaining := budget - time.Since(start)
	if remaining <= 0 {
		return nil
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// canaryOutcome is what awaitCanaryRunning observed about one canary job.
type canaryOutcome struct {
	// running is set once the job reached Running (or a later success), i.e. the scheduler placed
	// it on a node and the executor started it.
	running bool
	// failure describes a terminal event (failed, cancelled) that ended the wait early.
	failure string
	// last names the most recent event seen for the job, e.g. "JobQueuedEvent".
	last string
}

// awaitCanaryRunning reads jobSetId's event stream until jobId reaches Running, ends in a
// terminal failure, or timeout elapses. Reaching the timeout (or ctx being cancelled) is not an
// error: it returns the outcome so far with running unset.
func awaitCanaryRunning(ctx context.Context, eventClient api.EventClient, queue, jobSetId, jobId string, timeout time.Duration) (canaryOutcome, error) {
	var outcome canaryOutcome
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	stream, err := eventClient.GetJobSetEvents(ctx, &api.JobSetRequest{Queue: queue, Id: jobSetId, Watch: true})
	if err != nil {
		return outcome, fmt.Errorf("watching events for job set %s: %w", jobSetId, err)
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return outcome, nil
			}
			return outcome, fmt.Errorf("reading events for job %s: %w", jobId, err)
		}
		if msg == nil || msg.Message == nil {
			continue
		}
		event, err := api.UnwrapEvent(msg.Message)
		if err != nil || event.GetJobId() != jobId {
			continue
		}
		outcome.last = strings.TrimPrefix(fmt.Sprintf("%T", event), "*api.")
		switch e := event.(type) {
		case *api.JobRunningEvent, *api.JobSucceededEvent:
			outcome.running = true
			return outcome, nil
		case *api.JobFailedEvent:
			outcome.failure = "failed: " + e.Reason
			return outcome, nil
		case *api.JobCancelledEvent:
			outcome.failure = "was cancelled"
			return outcome, nil
		}
	}
}

func lastEventOrNone(last string) string {
	if last == "" {
		return "none"
	}
	return last
}

// schedulerView is what the scheduler's report for a job says about the nodes it considered.
type schedulerView struct {
	nodes    string   // "Number of nodes in cluster", "" if the report had none
	excluded []string // "<count>: <reason>" per exclusion reason
}

var (
	nodesInClusterRe = regexp.MustCompile(`Number of nodes in cluster:\s*(\d+)`)
	excludedNodeRe   = regexp.MustCompile(`(?m)^\s*(\d+):\s+(\S.*?)\s*$`)
)

// fetchSchedulerView returns the scheduler's report for jobId, parsed, or the zero value if it has
// none. Best effort: the scheduler keeps only its most recent round, so a job it hasn't looked at
// yet reports "none", and any failure to fetch it is swallowed since this only enriches an error
// that is already being returned.
func fetchSchedulerView(apiConnectionDetails *client.ApiConnectionDetails, jobId string) schedulerView {
	var report string
	_ = client.WithSchedulerReportingClient(apiConnectionDetails, func(reportingClient schedulerobjects.SchedulerReportingClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), schedulerReportTimeout)
		defer cancel()
		resp, err := reportingClient.GetJobReport(ctx, &schedulerobjects.JobReportRequest{JobId: jobId})
		if err != nil {
			return err
		}
		report = resp.Report
		return nil
	})
	return parseSchedulerReport(report)
}

// parseSchedulerReport pulls the node count and the per-reason exclusion counts out of a job report
// (the PodSchedulingContext text in internal/scheduler/scheduling/context/pod.go). Anything it does
// not recognise is left out, so a changed format degrades to a shorter message, not an error.
func parseSchedulerReport(report string) schedulerView {
	var view schedulerView
	if m := nodesInClusterRe.FindStringSubmatch(report); m != nil {
		view.nodes = m[1]
	}
	if i := strings.Index(report, "Excluded nodes:"); i >= 0 {
		for _, m := range excludedNodeRe.FindAllStringSubmatch(report[i:], -1) {
			view.excluded = append(view.excluded, m[1]+": "+m[2])
		}
	}
	return view
}

func submitCanaryJob(apiConnectionDetails *client.ApiConnectionDetails, queue, jobSetId, targetName string, selectTarget bool) (string, error) {
	var jobId string
	err := client.WithSubmitClient(apiConnectionDetails, func(submitClient api.SubmitClient) error {
		requests := client.CreateChunkedSubmitRequests(queue, jobSetId, []*api.JobSubmitRequestItem{canaryJobSpec(targetName, selectTarget)})
		for _, request := range requests {
			var response *api.JobSubmitResponse
			var err error
			for i := 0; i < submit.QueueVisibilityRetries; i++ {
				response, err = client.SubmitJobs(submitClient, request)
				if err == nil || status.Code(err) != codes.PermissionDenied {
					break
				}
				time.Sleep(submit.QueueVisibilityDelay)
			}
			if err != nil {
				return fmt.Errorf("submitting canary job: %w", err)
			}
			for _, item := range response.JobResponseItems {
				if item.Error != "" {
					return fmt.Errorf("canary job rejected: %s", item.Error)
				}
				jobId = item.JobId
			}
		}
		return nil
	})
	return jobId, err
}

func cancelCanaryJob(apiConnectionDetails *client.ApiConnectionDetails, queue, jobSetId, jobId string) {
	_ = client.WithSubmitClient(apiConnectionDetails, func(submitClient api.SubmitClient) error {
		_, err := submitClient.CancelJobs(context.Background(), &api.JobCancelRequest{
			JobId:    jobId,
			JobSetId: jobSetId,
			Queue:    queue,
		})
		return err
	})
}

func canaryJobSpec(targetName string, selectTarget bool) *api.JobSubmitRequestItem {
	cpu := resource.MustParse("10m")
	memory := resource.MustParse("8Mi")
	nodeSelector := map[string]string{NodeAnnotation: NodeAnnotationOK}
	if selectTarget {
		nodeSelector[TargetLabel] = targetName
	}
	return &api.JobSubmitRequestItem{
		Namespace: "default",
		PodSpec: &v1.PodSpec{
			TerminationGracePeriodSeconds: pointerTo(int64(0)),
			RestartPolicy:                 v1.RestartPolicyNever,
			NodeSelector:                  nodeSelector,
			Tolerations: []v1.Toleration{
				{
					Key:      NodeAnnotation,
					Operator: v1.TolerationOpEqual,
					Value:    NodeAnnotationOK,
					Effect:   v1.TaintEffectNoSchedule,
				},
			},
			Containers: []v1.Container{
				{
					Name:    "canary",
					Image:   "alpine:3.21.3",
					Command: []string{"sh"},
					Args:    []string{"-c", "sleep 10"},
					Resources: v1.ResourceRequirements{
						Limits:   v1.ResourceList{v1.ResourceCPU: cpu, v1.ResourceMemory: memory},
						Requests: v1.ResourceList{v1.ResourceCPU: cpu, v1.ResourceMemory: memory},
					},
				},
			},
		},
	}
}

func pointerTo[T any](v T) *T {
	return &v
}
