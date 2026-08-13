package service

import (
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/errormatch"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/executor/categorizer"
	"github.com/armadaproject/armada/internal/executor/configuration"
	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/metrics"
	"github.com/armadaproject/armada/internal/executor/podchecks"
	"github.com/armadaproject/armada/internal/executor/podchecks/failedpodchecks"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type podIssueType int

const (
	UnableToSchedule podIssueType = iota
	StuckStartingUp
	StuckTerminating
	ActiveDeadlineExceeded
	ExternallyDeleted
	ErrorDuringIssueHandling
	FailedStartingUp
	DeleteActionFailure
)

type podIssue struct {
	// Classification and DetectionTime are set for DeleteActionFailure issues.
	Classification categorizer.ClassifyResult
	DetectionTime  time.Time
	// A copy of the pod when an issue was detected
	OriginalPodState  *v1.Pod
	Message           string
	DebugMessage      string
	Retryable         bool
	DeletionRequested bool
	Type              podIssueType
	Cause             armadaevents.KubernetesReason
}

type reconciliationIssue struct {
	InitialDetectionTime time.Time
	OriginalRunState     *job.RunState
}

type issue struct {
	CurrentPodState *v1.Pod
	RunIssue        *runIssue
}

type runIssue struct {
	JobId               string
	RunId               string
	PodIssue            *podIssue
	ReconciliationIssue *reconciliationIssue
	Reported            bool
}

type IssueHandler interface {
	HasIssue(runId string) bool
	DetectAndRegisterIssuesForFailedPod(pod *v1.Pod) (bool, error)
}

type PodIssueHandler struct {
	clusterContext    executorContext.ClusterContext
	eventReporter     reporter.EventReporter
	pendingPodChecker podchecks.PodChecker
	failedPodChecker  failedpodchecks.RetryChecker
	stateChecksConfig configuration.StateChecksConfiguration
	classifier        *categorizer.Classifier
	debugRenderer     *reporter.DebugMessageRenderer

	stuckTerminatingPodExpiry time.Duration
	podKillTimeout            time.Duration

	// JobRunId -> PodIssue
	knownPodIssues map[string]*runIssue
	podIssueMutex  sync.Mutex
	// Run ids a terminated-run debug event has already been reported for. The event is diagnostic,
	// so losing this across an executor restart is acceptable.
	reportedTerminationDebugRunIds map[string]struct{}
	reportedTerminationDebugLock   sync.Mutex
	jobRunState                    job.RunStateStore
	clock                          clock.Clock
}

func NewPodIssuerHandler(
	jobRunState job.RunStateStore,
	clusterContext executorContext.ClusterContext,
	eventReporter reporter.EventReporter,
	stateChecksConfig configuration.StateChecksConfiguration,
	pendingPodChecker podchecks.PodChecker,
	failedPodChecker failedpodchecks.RetryChecker,
	stuckTerminatingPodExpiry time.Duration,
	podKillTimeout time.Duration,
	classifier *categorizer.Classifier,
	debugRenderer *reporter.DebugMessageRenderer,
) (*PodIssueHandler, error) {
	issueHandler := &PodIssueHandler{
		jobRunState:               jobRunState,
		clusterContext:            clusterContext,
		eventReporter:             eventReporter,
		pendingPodChecker:         pendingPodChecker,
		failedPodChecker:          failedPodChecker,
		stateChecksConfig:         stateChecksConfig,
		classifier:                classifier,
		debugRenderer:             debugRenderer,
		stuckTerminatingPodExpiry: stuckTerminatingPodExpiry,
		podKillTimeout:            podKillTimeout,
		knownPodIssues:            map[string]*runIssue{},
		podIssueMutex:             sync.Mutex{},
		clock:                     clock.RealClock{},

		reportedTerminationDebugRunIds: map[string]struct{}{},
		reportedTerminationDebugLock:   sync.Mutex{},
	}

	_, err := clusterContext.AddPodEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			// A deletion the watch missed arrives as a tombstone holding the last observed pod
			// state. That is good enough to measure termination, but too stale to judge whether the
			// deletion was unexpected, so it does not reach the issue detection below.
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				if pod, ok := tombstone.Obj.(*v1.Pod); ok {
					issueHandler.recordTerminationOverdue(pod)
				}
				return
			}
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			issueHandler.recordTerminationOverdue(pod)
			issueHandler.handleDeletedPod(pod)
		},
	})
	if err != nil {
		return nil, err
	}

	return issueHandler, nil
}

func (p *PodIssueHandler) HasIssue(runId string) bool {
	p.podIssueMutex.Lock()
	defer p.podIssueMutex.Unlock()

	if runId == "" {
		return false
	}

	_, exists := p.knownPodIssues[runId]
	return exists
}

func (p *PodIssueHandler) DetectAndRegisterFailedPodIssue(pod *v1.Pod) (bool, error) {
	if !util.IsManagedPod(pod) || pod.Status.Phase != v1.PodFailed {
		return false, nil
	}
	jobId := util.ExtractJobId(pod)
	runId := util.ExtractJobRunId(pod)

	podEvents, err := p.clusterContext.GetPodEvents(pod)
	if err != nil {
		return false, fmt.Errorf("Failed retrieving pod events for pod %s: %v", pod.Name, err)
	}

	isRetryable, message := p.failedPodChecker.IsRetryable(pod, podEvents)
	if isRetryable {
		return p.registerIssue(&runIssue{
			JobId: jobId,
			RunId: runId,
			PodIssue: &podIssue{
				OriginalPodState:  pod.DeepCopy(),
				Message:           message,
				DebugMessage:      p.debugRenderer.Render(pod, podEvents, reporter.TriggerPodFailed),
				Retryable:         true,
				DeletionRequested: false,
				Type:              FailedStartingUp,
			},
			Reported: false,
		})
	} else {
		return false, nil
	}
}

// DetectAndRegisterIssuesForFailedPod runs the failed pod detections in
// precedence order: the failed pod checks keep first claim on every pod while
// they are being deprecated in favour of categories, then the category delete
// action is considered.
func (p *PodIssueHandler) DetectAndRegisterIssuesForFailedPod(pod *v1.Pod) (bool, error) {
	issueAdded, err := p.DetectAndRegisterFailedPodIssue(pod)
	if issueAdded || err != nil {
		return issueAdded, err
	}
	return p.DetectAndRegisterDeleteActionIssue(pod)
}

// DetectAndRegisterDeleteActionIssue registers an issue for a failed pod whose
// matched category has action Delete.
func (p *PodIssueHandler) DetectAndRegisterDeleteActionIssue(pod *v1.Pod) (bool, error) {
	if !util.IsManagedPod(pod) || pod.Status.Phase != v1.PodFailed {
		return false, nil
	}
	podEvents, err := p.clusterContext.GetPodEvents(pod)
	if err != nil {
		// The events feed the debug message and the onPodEvents rules. Both
		// are best effort. The delete-first ordering is not. If the fetch
		// fails, classify and register without the events. The caller must
		// not report the failure while the pod still holds its name.
		log.Warnf("Failed retrieving pod events for pod %s: %v", pod.Name, err)
		podEvents = nil
	}
	// Classify with the extracted failure reason and the pod events. This
	// lets onPodError and onPodEvents rules match pods that never started a
	// container, for example kubelet admission rejections. Such pods have no
	// exit codes and no termination messages.
	failedReason := util.ExtractPodFailedReason(pod)
	classification := p.classifier.ClassifyPodError(pod, failedReason, podEvents)
	if classification.Action != categorizer.PodFailureActionDelete {
		return false, nil
	}
	return p.registerIssue(&runIssue{
		JobId: util.ExtractJobId(pod),
		RunId: util.ExtractJobRunId(pod),
		PodIssue: &podIssue{
			OriginalPodState: pod.DeepCopy(),
			Message:          failedReason,
			DebugMessage:     p.debugRenderer.Render(pod, podEvents, reporter.TriggerPodFailed),
			Retryable:        false,
			Type:             DeleteActionFailure,
			Cause:            util.ExtractPodFailureCause(pod),
			Classification:   classification,
			DetectionTime:    p.clock.Now(),
		},
		Reported: false,
	})
}

func (p *PodIssueHandler) registerIssue(issue *runIssue) (bool, error) {
	p.podIssueMutex.Lock()
	defer p.podIssueMutex.Unlock()

	runId := issue.RunId
	if runId == "" {
		return false, fmt.Errorf("Not registering an issue for job %s as run id was empty", issue.JobId)
	}
	_, exists := p.knownPodIssues[issue.RunId]
	if !exists {
		description := "unknown issue type"
		if issue.PodIssue != nil {
			description = fmt.Sprintf("pod issue - type %d - %s", issue.PodIssue.Type, issue.PodIssue.Message)
		} else if issue.ReconciliationIssue != nil {
			description = "reconciliation issue"
		}

		log.Infof("Issue for job %s run %s is registered - %s", issue.JobId, issue.RunId, description)
		p.knownPodIssues[issue.RunId] = issue
		return true, nil
	} else {
		log.Warnf("Not registering an issue for job %s (runId %s) as it already has an issue set", issue.JobId, issue.RunId)
		return false, nil
	}
}

func (p *PodIssueHandler) attemptToRegisterIssue(issue *runIssue) {
	_, err := p.registerIssue(issue)
	if err != nil {
		log.Warn(err)
	}
}

func (p *PodIssueHandler) markIssuesResolved(issue *runIssue) {
	p.podIssueMutex.Lock()
	defer p.podIssueMutex.Unlock()
	log.Infof("Issue for job %s run %s is resolved", issue.JobId, issue.RunId)

	delete(p.knownPodIssues, issue.RunId)
}

func (p *PodIssueHandler) markIssueReported(issue *runIssue) {
	issue.Reported = true
}

func (p *PodIssueHandler) HandlePodIssues() {
	managedPods, err := p.clusterContext.GetBatchPods()
	if err != nil {
		log.WithError(err).Errorf("unable to handle pod issus as failed to load pods")
	}
	p.detectPodIssues(managedPods)
	p.detectReconciliationIssues(managedPods)
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), time.Minute*2)
	defer cancel()
	p.handleKnownIssues(ctx, managedPods)
}

func (p *PodIssueHandler) detectPodIssues(allManagedPods []*v1.Pod) {
	for _, pod := range allManagedPods {
		if p.HasIssue(util.ExtractJobRunId(pod)) {
			continue
		}
		if util.IsInTerminalState(pod) {
			// No need to detect issues on completed pods
			// This prevents us sending updates on pods that are already finished and reported
			continue
		}
		if pod.DeletionTimestamp != nil && pod.DeletionTimestamp.Add(p.stuckTerminatingPodExpiry).Before(p.clock.Now()) {
			if util.IsMarkedForDeletion(pod) {
				// If the executor marked the pod for deletion, make sure the deletion logic is handling the pod
				// However don't handle it as a pod issue, as we don't want to send events about pods we're deleting
				p.reportTerminationDebugIfOverdue(pod)
				p.clusterContext.DeletePods([]*v1.Pod{pod})
				continue
			}

			// pod is stuck in terminating phase, this sometimes happen on node failure
			// it is safer to produce failed event than retrying as the job might have run already
			issue := &podIssue{
				OriginalPodState: pod.DeepCopy(),
				Message:          "job couldn't shut down cleanly as pod stuck in terminating phase, this indicates a node issue",
				DebugMessage:     p.renderTerminationDebugMessage(pod, reporter.TriggerStuckTerminating),
				Retryable:        false,
				Type:             StuckTerminating,
			}

			p.attemptToRegisterIssue(&runIssue{
				JobId:    util.ExtractJobId(pod),
				RunId:    util.ExtractJobRunId(pod),
				PodIssue: issue,
			})
		} else if p.hasExceededActiveDeadline(pod) {
			// Pod has past its active deadline seconds + some buffer.
			// As the pod is still here it means the kubelet is unable to kill it for some reason.
			// Start cleaning it up - which will eventually be force killed
			issue := &podIssue{
				OriginalPodState: pod.DeepCopy(),
				Message:          "pod has exceeded active deadline seconds",
				DebugMessage:     p.renderTerminationDebugMessage(pod, reporter.TriggerActiveDeadlineExceeded),
				Retryable:        false,
				Type:             ActiveDeadlineExceeded,
			}

			p.attemptToRegisterIssue(&runIssue{
				JobId:    util.ExtractJobId(pod),
				RunId:    util.ExtractJobRunId(pod),
				PodIssue: issue,
			})
		} else if pod.Status.Phase == v1.PodUnknown || pod.Status.Phase == v1.PodPending {

			podEvents, err := p.clusterContext.GetPodEvents(pod)
			if err != nil {
				log.Errorf("Unable to get pod events for pod %s: %v", pod.Name, err)
			}

			action, cause, podCheckMessage := p.pendingPodChecker.GetAction(pod, podEvents)

			if action != podchecks.ActionWait {
				retryable := action == podchecks.ActionRetry
				message := createStuckPodMessage(retryable, podCheckMessage)
				debugMessage := p.debugRenderer.Render(pod, podEvents, reporter.TriggerPodFailed)
				podIssueType := StuckStartingUp
				if cause == podchecks.NoNodeAssigned {
					podIssueType = UnableToSchedule
				}

				log.Infof("Found issue with pod %s in namespace %s: %s", pod.Name, pod.Namespace, message)

				issue := &podIssue{
					OriginalPodState: pod.DeepCopy(),
					Message:          message,
					DebugMessage:     debugMessage,
					Retryable:        retryable,
					Type:             podIssueType,
				}
				p.attemptToRegisterIssue(&runIssue{
					JobId:    util.ExtractJobId(pod),
					RunId:    util.ExtractJobRunId(pod),
					PodIssue: issue,
				})
			}
		}
	}
}

// This path is otherwise silent: the pod is being deleted deliberately, so nothing else reports on it.
//
// Triggered by the escalation deadline rather than by evidence of a force delete, which can lag the
// deadline by the repeat-deletion debounce and often only lands as the pod disappears.
func (p *PodIssueHandler) reportTerminationDebugIfOverdue(pod *v1.Pod) {
	if pod.DeletionTimestamp == nil {
		return
	}
	if !p.clock.Now().After(pod.DeletionTimestamp.Add(p.podKillTimeout)) {
		return
	}
	runId := util.ExtractJobRunId(pod)
	if runId == "" {
		return
	}
	// This event exists only to carry the payload, so there is nothing to report without one - which
	// is also how capture being disabled reaches here.
	debugMessage := p.renderTerminationDebugMessage(pod, reporter.TriggerStuckTerminating)
	if debugMessage == "" {
		return
	}
	if !p.claimRunForTerminationDebug(runId) {
		return
	}

	debugEvent, err := reporter.CreateJobRunTerminatedDebugEvent(pod, debugMessage)
	if err != nil {
		log.Errorf("Failed creating debug event for terminating pod %s: %v", pod.Name, err)
		return
	}
	// Queued rather than reported synchronously: a node failure can strand hundreds of pods in this
	// state at once, and detectPodIssues is a single loop that must not stall on a round trip per pod.
	p.eventReporter.QueueEvent(reporter.EventMessage{Event: debugEvent, JobRunId: runId}, func(err error) {
		if err != nil {
			log.Errorf("Failed reporting debug event for terminating pod %s: %v", pod.Name, err)
		}
	})
}

// Returns true the first time it is called for a run id. A pod that will not terminate is
// re-detected on every pod issue handling interval.
func (p *PodIssueHandler) claimRunForTerminationDebug(runId string) bool {
	p.reportedTerminationDebugLock.Lock()
	defer p.reportedTerminationDebugLock.Unlock()

	if _, reported := p.reportedTerminationDebugRunIds[runId]; reported {
		return false
	}
	p.reportedTerminationDebugRunIds[runId] = struct{}{}
	return true
}

func (p *PodIssueHandler) forgetTerminationDebug(runId string) {
	p.reportedTerminationDebugLock.Lock()
	defer p.reportedTerminationDebugLock.Unlock()
	delete(p.reportedTerminationDebugRunIds, runId)
}

func (p *PodIssueHandler) renderTerminationDebugMessage(pod *v1.Pod, trigger reporter.DebugTrigger) string {
	podEvents, err := p.clusterContext.GetPodEvents(pod)
	if err != nil {
		// The events are the richest part of the payload but the pod, node and termination state
		// still diagnose a stuck teardown, so render without them.
		log.Warnf("Failed retrieving pod events for terminating pod %s: %v", pod.Name, err)
		podEvents = nil
	}
	return p.debugRenderer.Render(pod, podEvents, trigger)
}

// Returns true if the pod has been running longer than its activeDeadlineSeconds + grace period
func (p *PodIssueHandler) hasExceededActiveDeadline(pod *v1.Pod) bool {
	if pod.Spec.ActiveDeadlineSeconds == nil {
		return false
	}

	// Using StartTime here, as kubernetes bases its activeDeadlineSeconds check on the StartTime also
	startTime := pod.Status.StartTime
	if startTime == nil || startTime.Time.IsZero() {
		return false
	}
	currentRunTimeSeconds := time.Now().Sub(startTime.Time).Seconds()

	podTerminationGracePeriodSeconds := float64(0)
	if pod.Spec.TerminationGracePeriodSeconds != nil {
		podTerminationGracePeriodSeconds = float64(*pod.Spec.TerminationGracePeriodSeconds)
	}
	deadline := float64(*pod.Spec.ActiveDeadlineSeconds) + podTerminationGracePeriodSeconds + p.stuckTerminatingPodExpiry.Seconds()
	return currentRunTimeSeconds > deadline
}

func (p *PodIssueHandler) handleKnownIssues(ctx *armadacontext.Context, allManagedPods []*v1.Pod) {
	// Make issues from pods + issues
	issues := createIssues(allManagedPods, p.knownPodIssues)
	util.ProcessItemsWithThreadPool(ctx, 20, issues, p.handleRunIssue)
}

func createIssues(managedPods []*v1.Pod, runIssues map[string]*runIssue) []*issue {
	podsByRunId := make(map[string]*v1.Pod, len(managedPods))

	for _, pod := range managedPods {
		runId := util.ExtractJobRunId(pod)
		if runId != "" {
			podsByRunId[runId] = pod
		} else {
			log.Warnf("failed to find run id for pod %s", pod.Name)
		}
	}

	result := make([]*issue, 0, len(runIssues))

	for _, runIssue := range runIssues {
		relatedPod := podsByRunId[runIssue.RunId]
		result = append(result, &issue{CurrentPodState: relatedPod, RunIssue: runIssue})
	}

	return result
}

func (p *PodIssueHandler) handleRunIssue(issue *issue) {
	if issue == nil || issue.RunIssue == nil {
		log.Warnf("issue found with missing issue details")
		return
	}
	if issue.RunIssue.PodIssue != nil {
		p.handlePodIssue(issue)
	} else if issue.RunIssue.ReconciliationIssue != nil {
		p.handleReconciliationIssue(issue)
	} else {
		log.Warnf("issue found with no issue details set for job %s run %s", issue.RunIssue.JobId, issue.RunIssue.RunId)
		p.markIssuesResolved(issue.RunIssue)
	}
}

func (p *PodIssueHandler) handlePodIssue(issue *issue) {
	hasSelfResolved := hasPodIssueSelfResolved(issue)
	if hasSelfResolved {
		log.Infof("Issue for job %s run %s has self resolved", issue.RunIssue.JobId, issue.RunIssue.RunId)
		p.markIssuesResolved(issue.RunIssue)
		return
	}

	if issue.RunIssue.PodIssue.Type == DeleteActionFailure {
		p.handleDeleteActionFailure(issue)
		return
	}

	if issue.RunIssue.PodIssue.Retryable {
		p.handleRetryableJobIssue(issue)
	} else {
		p.handleNonRetryableJobIssue(issue)
	}
}

// For non-retryable issues we must:
//   - Report JobUnableToScheduleEvent if the issue is a startup issue
//   - Report JobFailedEvent
//
// Once that is done we are free to cleanup the pod
func (p *PodIssueHandler) handleNonRetryableJobIssue(issue *issue) {
	if !issue.RunIssue.Reported {
		log.Infof("Handling non-retryable issue detected for job %s run %s", issue.RunIssue.JobId, issue.RunIssue.RunId)
		podIssue := issue.RunIssue.PodIssue
		clusterId := p.clusterContext.GetClusterId()

		var failureCategory, failureSubcategory, message string
		if sub := internalSubcategoryForPodIssueType(podIssue.Type); sub != "" {
			failureCategory, failureSubcategory = errormatch.CategoryInternal, sub
			message = podIssue.Message
		} else {
			result := p.classifier.ClassifyPodError(podIssue.OriginalPodState, podIssue.Message, nil)
			failureCategory, failureSubcategory = result.Category, result.Subcategory
			message = result.AppendHint(podIssue.Message)
		}

		failedEvent, err := reporter.CreateJobFailedEvent(
			podIssue.OriginalPodState,
			message,
			podIssue.Cause,
			podIssue.DebugMessage,
			util.ExtractFailedPodContainerStatuses(podIssue.OriginalPodState, clusterId),
			clusterId,
			failureCategory,
			failureSubcategory,
		)
		if err != nil {
			log.Errorf("Failed to create failed event for job %s because %s", issue.RunIssue.JobId, err)
			return
		}
		err = p.eventReporter.Report([]reporter.EventMessage{{Event: failedEvent, JobRunId: issue.RunIssue.RunId}})
		if err != nil {
			log.Errorf("Failed to report failed event for job %s because %s", issue.RunIssue.JobId, err)
			return
		}
		// Increment only after successful Report so failed sends do not inflate the counter.
		// RecordJobFailure is a no-op when classification didn't run (empty category).
		metrics.RecordJobFailure(failureCategory, failureSubcategory)
		p.markIssueReported(issue.RunIssue)
	}

	if issue.CurrentPodState != nil {
		p.clusterContext.DeletePods([]*v1.Pod{issue.CurrentPodState})
		issue.RunIssue.PodIssue.DeletionRequested = true
	} else {
		p.markIssuesResolved(issue.RunIssue)
	}
}

// internalSubcategoryForPodIssueType returns the internal failure subcategory
// for Armada-detected structural pod issues. It returns "" for StuckStartingUp,
// UnableToSchedule, and FailedStartingUp, whose cause (e.g. image pull, scheduling)
// the operator categorizer should attribute instead.
func internalSubcategoryForPodIssueType(t podIssueType) string {
	switch t {
	case StuckTerminating:
		return errormatch.SubcategoryStuckTerminating
	case ExternallyDeleted:
		return errormatch.SubcategoryExternallyDeleted
	case ErrorDuringIssueHandling:
		return errormatch.SubcategoryIssueHandlerError
	case ActiveDeadlineExceeded:
		return errormatch.SubcategoryActiveDeadline
	default:
		return ""
	}
}

// handleDeleteActionFailure deletes a failed pod whose category action is
// Delete and reports the terminal categorized failure only once the pod is
// confirmed gone from the cluster. The report is what triggers a scheduler
// retry, so this ordering guarantees the retry can never collide with the old
// pod's name. A pod that cannot be deleted within stuckTerminatingPodExpiry is
// instead failed terminally as an internal stuck-terminating error, which is
// safer than leaving the job invisible forever. The issue stays registered
// until the pod is actually gone, so the report fires exactly once and the
// lingering pod cannot be re-detected as a fresh failure.
func (p *PodIssueHandler) handleDeleteActionFailure(issue *issue) {
	podIssue := issue.RunIssue.PodIssue

	if issue.CurrentPodState == nil {
		if !issue.RunIssue.Reported {
			p.reportDeleteActionFailure(issue, podIssue.Classification.Category, podIssue.Classification.Subcategory,
				podIssue.Classification.AppendHint(podIssue.Message))
		}
		if issue.RunIssue.Reported {
			p.markIssuesResolved(issue.RunIssue)
		}
		return
	}

	if !issue.RunIssue.Reported {
		deleteStartedAt := podIssue.DetectionTime
		if issue.CurrentPodState.DeletionTimestamp != nil {
			deleteStartedAt = issue.CurrentPodState.DeletionTimestamp.Time
		}
		if deleteStartedAt.Add(p.stuckTerminatingPodExpiry).Before(p.clock.Now()) {
			// Re-render against the pod as it is now, so the failure carries why the delete did not
			// take effect. This is strictly richer than what was captured at detection time.
			podIssue.DebugMessage = p.renderTerminationDebugMessage(issue.CurrentPodState, reporter.TriggerUndeletable)
			p.reportDeleteActionFailure(issue, errormatch.CategoryInternal, errormatch.SubcategoryStuckTerminating,
				podIssue.Message+"\n\nThe failed pod could not be deleted, so the job is failed rather than retried.")
		}
	}

	err := p.clusterContext.DeletePodWithCondition(issue.CurrentPodState, func(pod *v1.Pod) bool {
		return pod.Status.Phase == v1.PodFailed
	}, true)
	if err != nil {
		log.Errorf("Failed to delete failed pod of job %s because %s", issue.RunIssue.JobId, err)
		return
	}
	podIssue.DeletionRequested = true
}

func (p *PodIssueHandler) reportDeleteActionFailure(issue *issue, category string, subcategory string, message string) {
	podIssue := issue.RunIssue.PodIssue
	clusterId := p.clusterContext.GetClusterId()
	failedEvent, err := reporter.CreateJobFailedEvent(
		podIssue.OriginalPodState,
		message,
		podIssue.Cause,
		podIssue.DebugMessage,
		util.ExtractFailedPodContainerStatuses(podIssue.OriginalPodState, clusterId),
		clusterId,
		category,
		subcategory,
	)
	if err != nil {
		log.Errorf("Failed to create failed event for job %s because %s", issue.RunIssue.JobId, err)
		return
	}
	err = p.eventReporter.Report([]reporter.EventMessage{{Event: failedEvent, JobRunId: issue.RunIssue.RunId}})
	if err != nil {
		log.Errorf("Failed to report failed event for job %s because %s", issue.RunIssue.JobId, err)
		return
	}
	metrics.RecordJobFailure(category, subcategory)
	p.markIssueReported(issue.RunIssue)
}

// For retryable issues we must:
//   - Report JobReturnLeaseEvent, carrying the pod error classification when a rule matches
//
// If the pod becomes Running/Completed/Failed in the middle of being deleted - swap this issue to a nonRetryableIssue where it will be Failed
func (p *PodIssueHandler) handleRetryableJobIssue(issue *issue) {
	log.Infof("Handling retryable issue for job %s run %s", issue.RunIssue.JobId, issue.RunIssue.RunId)
	if issue.CurrentPodState != nil {
		if issue.RunIssue.PodIssue.OriginalPodState.Status.Phase == v1.PodPending && issue.CurrentPodState.Status.Phase != v1.PodPending {
			p.markIssuesResolved(issue.RunIssue)
			if issue.RunIssue.PodIssue.DeletionRequested {
				p.attemptToRegisterIssue(&runIssue{
					JobId: issue.RunIssue.JobId,
					RunId: issue.RunIssue.RunId,
					PodIssue: &podIssue{
						OriginalPodState: issue.RunIssue.PodIssue.OriginalPodState,
						Message: fmt.Sprintf("Pod unexpectedly started up after delete was called.\n\nDelete was originally called to handle issue:\n%s",
							issue.RunIssue.PodIssue.Message),
						Retryable:         false,
						DeletionRequested: false,
						Type:              ErrorDuringIssueHandling,
						Cause:             armadaevents.KubernetesReason_AppError,
					},
				})
			}
			return
		}

		err := p.clusterContext.DeletePodWithCondition(issue.CurrentPodState, func(pod *v1.Pod) bool {
			return pod.Status.Phase == issue.RunIssue.PodIssue.OriginalPodState.Status.Phase
		}, true)
		if err != nil {
			log.Errorf("Failed to delete pod of running job %s because %s", issue.RunIssue.JobId, err)
			return
		} else {
			issue.RunIssue.PodIssue.DeletionRequested = true
		}
	} else {
		// TODO
		// When we have our own internal state - we don't need to wait for the pod deletion to complete
		// We can just mark is to delete in our state and return the lease
		jobRunAttempted := issue.RunIssue.PodIssue.Type != UnableToSchedule
		result := p.classifier.ClassifyPodError(issue.RunIssue.PodIssue.OriginalPodState, issue.RunIssue.PodIssue.Message, nil)

		returnLeaseEvent, err := reporter.CreateReturnLeaseEvent(
			issue.RunIssue.PodIssue.OriginalPodState,
			result.AppendHint(issue.RunIssue.PodIssue.Message),
			issue.RunIssue.PodIssue.DebugMessage,
			p.clusterContext.GetClusterId(),
			jobRunAttempted,
			result.Category,
			result.Subcategory,
		)
		if err != nil {
			log.Errorf("Failed to create return lease event for job %s because %s", issue.RunIssue.JobId, err)
			return
		}

		err = p.eventReporter.Report([]reporter.EventMessage{{Event: returnLeaseEvent, JobRunId: issue.RunIssue.RunId}})
		if err != nil {
			log.Errorf("Failed to return lease for job %s because %s", issue.RunIssue.JobId, err)
			return
		}
		// Record only after a successful Report so failed sends do not inflate the counter.
		metrics.RecordJobFailure(result.Category, result.Subcategory)
		p.markIssuesResolved(issue.RunIssue)
	}
}

func hasPodIssueSelfResolved(issue *issue) bool {
	if issue == nil || issue.RunIssue == nil || issue.RunIssue.PodIssue == nil {
		return true
	}

	isStuckStartingUpAndResolvable := issue.RunIssue.PodIssue.Type == StuckStartingUp &&
		(issue.RunIssue.PodIssue.Retryable || (!issue.RunIssue.PodIssue.Retryable && !issue.RunIssue.Reported))
	if issue.RunIssue.PodIssue.Type == UnableToSchedule || isStuckStartingUpAndResolvable {
		// If pod has disappeared - don't consider it resolved as we still need to report the issue
		if issue.CurrentPodState == nil {
			return false
		}

		// Pod has started up and we haven't tried to delete the pod yet - so resolve the issue
		if issue.CurrentPodState.Status.Phase != v1.PodPending && !issue.RunIssue.PodIssue.DeletionRequested {
			return true
		}
	}

	return false
}

func createStuckPodMessage(retryable bool, originalMessage string) string {
	if retryable {
		return fmt.Sprintf("Unable to start pod.\n%s", originalMessage)
	}
	return fmt.Sprintf("Unable to start pod - encountered an unrecoverable problem.\n%s", originalMessage)
}

// Observed where the pod actually disappears rather than where Armada decides to force delete it, so
// the value describes kubelet and node behaviour rather than Armada's own polling intervals, and
// every terminating pod is counted - not just the pathological tail that gets force deleted.
func (p *PodIssueHandler) recordTerminationOverdue(pod *v1.Pod) {
	if !util.IsManagedPod(pod) || pod.DeletionTimestamp == nil {
		return
	}
	metrics.RecordPodTerminationOverdue(util.ExtractPool(pod), p.clock.Now().Sub(pod.DeletionTimestamp.Time))
}

func (p *PodIssueHandler) handleDeletedPod(pod *v1.Pod) {
	p.forgetTerminationDebug(util.ExtractJobRunId(pod))

	jobId := util.ExtractJobId(pod)
	if jobId != "" {
		isUnexpectedDeletion := !util.IsMarkedForDeletion(pod) && !util.IsPodFinishedAndReported(pod)
		if isUnexpectedDeletion {
			p.attemptToRegisterIssue(&runIssue{
				JobId: jobId,
				RunId: util.ExtractJobRunId(pod),
				PodIssue: &podIssue{
					OriginalPodState: pod.DeepCopy(),
					Message:          "Pod was unexpectedly deleted",
					// Who deleted the pod is answered by the node - a drain, an eviction or a
					// foreign preemption. The pod is already gone, but its events and its node are
					// still in the informer caches.
					DebugMessage: p.renderTerminationDebugMessage(pod, reporter.TriggerExternallyDeleted),
					Retryable:    false,
					Type:         ExternallyDeleted,
				},
			})
		}
	}
}

func (p *PodIssueHandler) handleReconciliationIssue(issue *issue) {
	if issue.RunIssue.ReconciliationIssue == nil {
		log.Warnf("unexpected trying to process an issue as a reconciliation issue for job %s run %s", issue.RunIssue.JobId, issue.RunIssue.RunId)
		p.markIssuesResolved(issue.RunIssue)
		return
	}

	currentRunState := p.jobRunState.Get(issue.RunIssue.RunId)
	if currentRunState == nil {
		// No run for the run id - so there isn't a reconciliation issue
		p.markIssuesResolved(issue.RunIssue)
		return
	}

	if issue.CurrentPodState != nil {
		p.markIssuesResolved(issue.RunIssue)
		return
	}

	if issue.RunIssue.ReconciliationIssue.OriginalRunState.Phase != currentRunState.Phase || currentRunState.CancelRequested || currentRunState.PreemptionRequested {
		// State of the run has changed - resolve
		// If there is still an issue, it'll be re-detected
		p.markIssuesResolved(issue.RunIssue)
		return
	}

	timeSinceInitialDetection := p.clock.Now().Sub(issue.RunIssue.ReconciliationIssue.InitialDetectionTime)

	// If there is an active run and the associated pod has been missing for more than a given time period, report the run as failed
	if currentRunState.Phase == job.Active && timeSinceInitialDetection > p.stateChecksConfig.DeadlineForActivePodConsideredMissing {
		log.Infof("Pod missing for active run  detected for job %s run %s", issue.RunIssue.JobId, issue.RunIssue.RunId)

		event, err := reporter.CreateMinimalJobFailedEvent(
			currentRunState.Meta.JobId,
			issue.RunIssue.RunId,
			currentRunState.Meta.JobSet,
			currentRunState.Meta.Queue,
			p.clusterContext.GetClusterId(),
			"Pod is unexpectedly missing in Kubernetes",
			errormatch.CategoryInternal,
			errormatch.SubcategoryPodMissing,
		)
		if err != nil {
			log.Errorf("failed to create job failed event because %s", err)
			return
		}

		err = p.eventReporter.Report([]reporter.EventMessage{{Event: event, JobRunId: issue.RunIssue.RunId}})
		if err != nil {
			log.Errorf("Failure to report failed event %+v because %s", event, err)
			return
		}

		p.markIssueReported(issue.RunIssue)
		p.markIssuesResolved(issue.RunIssue)
	} else if currentRunState.Phase == job.SuccessfulSubmission && timeSinceInitialDetection > p.stateChecksConfig.DeadlineForSubmittedPodConsideredMissing {
		// If a pod hasn't shown up after a successful submission for a given time period, delete it from the run state
		// This will cause it to be re-leased and submitted again
		// If the issue is we are out of sync with kubernetes, the second submission will fail and kill the job
		p.jobRunState.Delete(currentRunState.Meta.RunId)
		p.markIssuesResolved(issue.RunIssue)
	}
}

func (p *PodIssueHandler) detectReconciliationIssues(pods []*v1.Pod) {
	runs := p.jobRunState.GetAllWithFilter(func(state *job.RunState) bool {
		return (state.Phase == job.Active || state.Phase == job.SuccessfulSubmission) && !state.CancelRequested && !state.PreemptionRequested
	})

	runIdsToPod := make(map[string]*v1.Pod, len(pods))
	for _, pod := range pods {
		runId := util.ExtractJobRunId(pod)
		if runId != "" {
			runIdsToPod[runId] = pod
		}
	}

	for _, run := range runs {
		_, present := runIdsToPod[run.Meta.RunId]
		if !present {
			if p.HasIssue(run.Meta.RunId) {
				continue
			}
			p.attemptToRegisterIssue(&runIssue{
				JobId: run.Meta.JobId,
				RunId: run.Meta.RunId,
				ReconciliationIssue: &reconciliationIssue{
					InitialDetectionTime: p.clock.Now(),
					OriginalRunState:     run.DeepCopy(),
				},
			})
		}
	}
}
