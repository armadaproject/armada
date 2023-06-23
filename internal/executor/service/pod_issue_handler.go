package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/job"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/client-go/tools/cache"

	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/podchecks"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/api"
)

type podIssueType int

const (
	UnableToSchedule podIssueType = iota
	StuckStartingUp
	StuckTerminating
	ExternallyDeleted
)

type podIssue struct {
	// A copy of the pod when an issue was detected
	OriginalPodState  *v1.Pod
	Message           string
	Retryable         bool
	DeletionRequested bool
	Type              podIssueType
	Cause             api.Cause
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

type IssueHandler struct {
	clusterContext    executorContext.ClusterContext
	eventReporter     reporter.EventReporter
	pendingPodChecker podchecks.PodChecker
	stateChecksConfig configuration.StateChecksConfiguration

	stuckTerminatingPodExpiry time.Duration

	// JobRunId -> PodIssue
	knownPodIssues map[string]*runIssue
	podIssueMutex  sync.Mutex
	jobRunState    job.RunStateStore
	clock          clock.Clock
}

func NewIssueHandler(
	jobRunState job.RunStateStore,
	clusterContext executorContext.ClusterContext,
	eventReporter reporter.EventReporter,
	stateChecksConfig configuration.StateChecksConfiguration,
	pendingPodChecker podchecks.PodChecker,
	stuckTerminatingPodExpiry time.Duration,
) *IssueHandler {
	issueHandler := &IssueHandler{
		jobRunState:               jobRunState,
		clusterContext:            clusterContext,
		eventReporter:             eventReporter,
		pendingPodChecker:         pendingPodChecker,
		stateChecksConfig:         stateChecksConfig,
		stuckTerminatingPodExpiry: stuckTerminatingPodExpiry,
		knownPodIssues:            map[string]*runIssue{},
		podIssueMutex:             sync.Mutex{},
		clock:                     clock.RealClock{},
	}

	clusterContext.AddPodEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			issueHandler.handleDeletedPod(pod)
		},
	})

	return issueHandler
}

func (p *IssueHandler) registerIssue(issue *runIssue) {
	p.podIssueMutex.Lock()
	defer p.podIssueMutex.Unlock()

	runId := issue.RunId
	if runId == "" {
		log.Warnf("Not registering an issue for job %s as run id was empty", issue.JobId)
		return
	}
	_, exists := p.knownPodIssues[issue.RunId]
	if !exists {
		p.knownPodIssues[issue.RunId] = issue
	} else {
		log.Warnf("Not registering an issue for job %s (runId %s) as it already has an issue set", issue.JobId, issue.RunId)
	}
}

func (p *IssueHandler) markIssuesResolved(issue *runIssue) {
	p.podIssueMutex.Lock()
	defer p.podIssueMutex.Unlock()

	delete(p.knownPodIssues, issue.RunId)
}

func (p *IssueHandler) markIssueReported(issue *runIssue) {
	issue.Reported = true
}

func (p *IssueHandler) HandlePodIssues() {
	managedPods, err := p.clusterContext.GetBatchPods()
	if err != nil {
		log.WithError(err).Errorf("unable to handle pod issus as failed to load pods")
	}
	managedPods = util.FilterPods(managedPods, func(pod *v1.Pod) bool {
		return !util.IsLegacyManagedPod(pod)
	})
	p.detectPodIssues(managedPods)
	p.detectReconciliationIssues(managedPods)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()
	p.handleKnownIssues(ctx, managedPods)
}

func (p *IssueHandler) detectPodIssues(allManagedPods []*v1.Pod) {
	for _, pod := range allManagedPods {
		if pod.DeletionTimestamp != nil && pod.DeletionTimestamp.Add(p.stuckTerminatingPodExpiry).Before(p.clock.Now()) {
			// pod is stuck in terminating phase, this sometimes happen on node failure
			// it is safer to produce failed event than retrying as the job might have run already
			issue := &podIssue{
				OriginalPodState: pod.DeepCopy(),
				Message:          "pod stuck in terminating phase, this might be due to platform problems",
				Retryable:        false,
				Type:             StuckTerminating,
			}

			p.registerIssue(&runIssue{
				JobId:    util.ExtractJobId(pod),
				RunId:    util.ExtractJobRunId(pod),
				PodIssue: issue,
			})
		} else if pod.Status.Phase == v1.PodUnknown || pod.Status.Phase == v1.PodPending {

			podEvents, err := p.clusterContext.GetPodEvents(pod)
			if err != nil {
				log.Errorf("Unable to get pod events for pod %s: %v", pod.Name, err)
			}

			lastStateChange, err := util.LastStatusChange(pod)
			if err != nil {
				log.Errorf("Unable to get lastStateChange for pod %s: %v", pod.Name, err)
				continue
			}

			action, cause, podCheckMessage := p.pendingPodChecker.GetAction(pod, podEvents, p.clock.Now().Sub(lastStateChange))

			if action != podchecks.ActionWait {
				retryable := action == podchecks.ActionRetry
				message := createStuckPodMessage(retryable, podCheckMessage)
				podIssueType := StuckStartingUp
				if cause == podchecks.NoNodeAssigned {
					podIssueType = UnableToSchedule
				}

				log.Warnf("Found issue with pod %s in namespace %s: %s", pod.Name, pod.Namespace, message)

				issue := &podIssue{
					OriginalPodState: pod.DeepCopy(),
					Message:          message,
					Retryable:        retryable,
					Type:             podIssueType,
				}
				p.registerIssue(&runIssue{
					JobId:    util.ExtractJobId(pod),
					RunId:    util.ExtractJobRunId(pod),
					PodIssue: issue,
				})
			}
		}
	}
}

func (p *IssueHandler) handleKnownIssues(ctx context.Context, allManagedPods []*v1.Pod) {
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

func (p *IssueHandler) handleRunIssue(issue *issue) {
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

func (p *IssueHandler) handlePodIssue(issue *issue) {
	hasSelfResolved := hasPodIssueSelfResolved(issue)
	if hasSelfResolved {
		log.Infof("Issue for job %s run %s has self resolved", issue.RunIssue.JobId, issue.RunIssue.RunId)
		p.markIssuesResolved(issue.RunIssue)
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
func (p *IssueHandler) handleNonRetryableJobIssue(issue *issue) {
	if !issue.RunIssue.Reported {
		log.Infof("Non-retryable issue detected for job %s run %s - %s", issue.RunIssue.JobId, issue.RunIssue.RunId, issue.RunIssue.PodIssue.Message)
		message := issue.RunIssue.PodIssue.Message

		events := make([]reporter.EventMessage, 0, 2)
		if issue.RunIssue.PodIssue.Type == StuckStartingUp || issue.RunIssue.PodIssue.Type == UnableToSchedule {
			unableToScheduleEvent := reporter.CreateJobUnableToScheduleEvent(issue.RunIssue.PodIssue.OriginalPodState, message, p.clusterContext.GetClusterId())
			events = append(events, reporter.EventMessage{Event: unableToScheduleEvent, JobRunId: issue.RunIssue.RunId})
		}
		failedEvent := reporter.CreateSimpleJobFailedEvent(issue.RunIssue.PodIssue.OriginalPodState, message, p.clusterContext.GetClusterId(), issue.RunIssue.PodIssue.Cause)
		events = append(events, reporter.EventMessage{Event: failedEvent, JobRunId: issue.RunIssue.RunId})

		err := p.eventReporter.Report(events)
		if err != nil {
			log.Errorf("Failed to report failed event for job %s because %s", issue.RunIssue.JobId, err)
			return
		}
		p.markIssueReported(issue.RunIssue)
	}

	if issue.CurrentPodState != nil {
		p.clusterContext.DeletePods([]*v1.Pod{issue.CurrentPodState})
		issue.RunIssue.PodIssue.DeletionRequested = true
	} else {
		p.markIssuesResolved(issue.RunIssue)
	}
}

// For retryable issues we must:
//   - Report JobUnableToScheduleEvent
//   - Report JobReturnLeaseEvent
//
// Special consideration must be taken that most of these pods are somewhat "stuck" in pending.
// So can transition to Running/Completed/Failed in the middle of this
// We must not return the lease if the pod state changes - as likely it has become "unstuck"
func (p *IssueHandler) handleRetryableJobIssue(issue *issue) {
	if !issue.RunIssue.Reported {
		log.Infof("Retryable issue detected for job %s run %s - %s", issue.RunIssue.JobId, issue.RunIssue.RunId, issue.RunIssue.PodIssue.Message)
		if issue.RunIssue.PodIssue.Type == StuckStartingUp || issue.RunIssue.PodIssue.Type == UnableToSchedule {
			event := reporter.CreateJobUnableToScheduleEvent(issue.RunIssue.PodIssue.OriginalPodState, issue.RunIssue.PodIssue.Message, p.clusterContext.GetClusterId())
			err := p.eventReporter.Report([]reporter.EventMessage{{Event: event, JobRunId: issue.RunIssue.RunId}})
			if err != nil {
				log.Errorf("Failure to report stuck pod event %+v because %s", event, err)
				return
			}
		}
		p.markIssueReported(issue.RunIssue)
	}

	if issue.CurrentPodState != nil {
		// TODO consider moving this to a synchronous call - but long termination periods would need to be handled
		err := p.clusterContext.DeletePodWithCondition(issue.CurrentPodState, func(pod *v1.Pod) bool {
			return pod.Status.Phase == v1.PodPending
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
		returnLeaseEvent := reporter.CreateReturnLeaseEvent(issue.RunIssue.PodIssue.OriginalPodState, issue.RunIssue.PodIssue.Message, p.clusterContext.GetClusterId(), jobRunAttempted)
		err := p.eventReporter.Report([]reporter.EventMessage{{Event: returnLeaseEvent, JobRunId: issue.RunIssue.RunId}})
		if err != nil {
			log.Errorf("Failed to return lease for job %s because %s", issue.RunIssue.JobId, err)
			return
		}
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

		// Pod has completed - no need to report any issues
		if util.IsInTerminalState(issue.CurrentPodState) {
			return true
		}

		// Pod has started running, and we haven't requested deletion - let it continue
		if issue.CurrentPodState.Status.Phase == v1.PodRunning && !issue.RunIssue.PodIssue.DeletionRequested {
			return true
		}
		// TODO There is an edge case here where the pod has started running but we have requested deletion
		// Without a proper state model, we can't easily handle this correctly
		// Ideally we'd see if it completes or deletes first and report it accordingly
		// If it completes first - do nothing
		// If it deletes first - report JobFailed (as we accidentally deleted it during the run)
	}

	return false
}

func createStuckPodMessage(retryable bool, originalMessage string) string {
	if retryable {
		return fmt.Sprintf("Unable to schedule pod, Armada will return lease and retry.\n%s", originalMessage)
	}
	return fmt.Sprintf("Unable to schedule pod with unrecoverable problem, Armada will not retry.\n%s", originalMessage)
}

func (p *IssueHandler) handleDeletedPod(pod *v1.Pod) {
	jobId := util.ExtractJobId(pod)
	if jobId != "" {
		isUnexpectedDeletion := !util.IsMarkedForDeletion(pod) && !util.IsPodFinishedAndReported(pod)
		if isUnexpectedDeletion {
			p.registerIssue(&runIssue{
				JobId: jobId,
				RunId: util.ExtractJobRunId(pod),
				PodIssue: &podIssue{
					OriginalPodState: pod,
					Message:          "Pod was unexpectedly deleted",
					Retryable:        false,
					Type:             ExternallyDeleted,
				},
			})
		}
	}
}

func (p *IssueHandler) handleReconciliationIssue(issue *issue) {
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

		event := &api.JobFailedEvent{
			JobId:     currentRunState.Meta.JobId,
			JobSetId:  currentRunState.Meta.JobSet,
			Queue:     currentRunState.Meta.Queue,
			Created:   p.clock.Now(),
			ClusterId: p.clusterContext.GetClusterId(),
			Reason:    fmt.Sprintf("Pod is unexpectedly missing in Kubernetes"),
			Cause:     api.Cause_Error,
		}

		err := p.eventReporter.Report([]reporter.EventMessage{{Event: event, JobRunId: issue.RunIssue.RunId}})
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

func (p *IssueHandler) detectReconciliationIssues(pods []*v1.Pod) {
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
			p.registerIssue(&runIssue{
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
