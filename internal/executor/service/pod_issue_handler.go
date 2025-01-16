package service

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubectl/pkg/describe"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/executor/configuration"
	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/job"
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
)

type podIssue struct {
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
	DetectAndRegisterFailedPodIssue(pod *v1.Pod) (bool, error)
}

type PodIssueHandler struct {
	clusterContext    executorContext.ClusterContext
	eventReporter     reporter.EventReporter
	pendingPodChecker podchecks.PodChecker
	failedPodChecker  failedpodchecks.RetryChecker
	stateChecksConfig configuration.StateChecksConfiguration

	stuckTerminatingPodExpiry time.Duration

	// JobRunId -> PodIssue
	knownPodIssues map[string]*runIssue
	podIssueMutex  sync.Mutex
	jobRunState    job.RunStateStore
	clock          clock.Clock
}

func NewPodIssuerHandler(
	jobRunState job.RunStateStore,
	clusterContext executorContext.ClusterContext,
	eventReporter reporter.EventReporter,
	stateChecksConfig configuration.StateChecksConfiguration,
	pendingPodChecker podchecks.PodChecker,
	failedPodChecker failedpodchecks.RetryChecker,
	stuckTerminatingPodExpiry time.Duration,
) (*PodIssueHandler, error) {
	issueHandler := &PodIssueHandler{
		jobRunState:               jobRunState,
		clusterContext:            clusterContext,
		eventReporter:             eventReporter,
		pendingPodChecker:         pendingPodChecker,
		failedPodChecker:          failedPodChecker,
		stateChecksConfig:         stateChecksConfig,
		stuckTerminatingPodExpiry: stuckTerminatingPodExpiry,
		knownPodIssues:            map[string]*runIssue{},
		podIssueMutex:             sync.Mutex{},
		clock:                     clock.RealClock{},
	}

	_, err := clusterContext.AddPodEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
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
				DebugMessage:      createDebugMessage(podEvents),
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

func (p *PodIssueHandler) registerIssue(issue *runIssue) (bool, error) {
	p.podIssueMutex.Lock()
	defer p.podIssueMutex.Unlock()

	runId := issue.RunId
	if runId == "" {
		return false, fmt.Errorf("Not registering an issue for job %s as run id was empty", issue.JobId)
	}
	_, exists := p.knownPodIssues[issue.RunId]
	if !exists {
		log.Infof("Issue for job %s run %s is registered", issue.JobId, issue.RunId)
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
		if util.IsInTerminalState(pod) && util.HasCurrentStateBeenReported(pod) {
			// No need to detect issues on completed pods
			// This prevents us sending updates on pods that are already finished and reported
			continue
		}
		if pod.DeletionTimestamp != nil && pod.DeletionTimestamp.Add(p.stuckTerminatingPodExpiry).Before(p.clock.Now()) {
			// pod is stuck in terminating phase, this sometimes happen on node failure
			// it is safer to produce failed event than retrying as the job might have run already
			issue := &podIssue{
				OriginalPodState: pod.DeepCopy(),
				Message:          "pod stuck in terminating phase, this might be due to platform problems",
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

			lastStateChange, err := util.LastStatusChange(pod)
			if err != nil {
				log.Errorf("Unable to get lastStateChange for pod %s: %v", pod.Name, err)
				continue
			}

			action, cause, podCheckMessage := p.pendingPodChecker.GetAction(pod, podEvents, p.clock.Now().Sub(lastStateChange))

			if action != podchecks.ActionWait {
				retryable := action == podchecks.ActionRetry
				message := createStuckPodMessage(retryable, podCheckMessage)
				debugMessage := createDebugMessage(podEvents)
				podIssueType := StuckStartingUp
				if cause == podchecks.NoNodeAssigned {
					podIssueType = UnableToSchedule
				}

				log.Warnf("Found issue with pod %s in namespace %s: %s", pod.Name, pod.Namespace, message)

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

func createDebugMessage(podEvents []*v1.Event) string {
	events := make([]v1.Event, 0, len(podEvents))
	for _, e := range podEvents {
		events = append(events, *e)
	}

	eventList := v1.EventList{Items: events}
	writer := bytes.Buffer{}
	prefixWriter := describe.NewPrefixWriter(&writer)

	describe.DescribeEvents(&eventList, prefixWriter)
	return writer.String()
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

		failedEvent, err := reporter.CreateSimpleJobFailedEvent(podIssue.OriginalPodState, podIssue.Message, podIssue.DebugMessage, p.clusterContext.GetClusterId(), podIssue.Cause)
		if err != nil {
			log.Errorf("Failed to create failed event for job %s because %s", issue.RunIssue.JobId, err)
			return
		}
		err = p.eventReporter.Report([]reporter.EventMessage{{Event: failedEvent, JobRunId: issue.RunIssue.RunId}})
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
//   - Report JobReturnLeaseEvent
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

		returnLeaseEvent, err := reporter.CreateReturnLeaseEvent(
			issue.RunIssue.PodIssue.OriginalPodState,
			issue.RunIssue.PodIssue.Message,
			issue.RunIssue.PodIssue.DebugMessage,
			p.clusterContext.GetClusterId(),
			jobRunAttempted,
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

func (p *PodIssueHandler) handleDeletedPod(pod *v1.Pod) {
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
					Retryable:        false,
					Type:             ExternallyDeleted,
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
			fmt.Sprintf("Pod is unexpectedly missing in Kubernetes"),
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
