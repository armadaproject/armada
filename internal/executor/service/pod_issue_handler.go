package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/podchecks"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/api"
)

type IssueType int

const (
	UnableToSchedule IssueType = iota
	StuckStartingUp
	StuckTerminating
	ExternallyDeleted
)

type podIssue struct {
	// A copy of the pod when an issue was detected
	OriginalPodState *v1.Pod
	JobId            string
	RunId            string
	Message          string
	Retryable        bool
	Reported         bool
	Type             IssueType
	Cause            api.Cause
}

type issue struct {
	CurrentPodState *v1.Pod
	Issue           *podIssue
}

type PodIssueService struct {
	clusterContext    executorContext.ClusterContext
	eventReporter     reporter.EventReporter
	pendingPodChecker podchecks.PodChecker

	stuckTerminatingPodExpiry time.Duration

	// JobRunId -> PodIssue
	knownPodIssues map[string]*podIssue
	podIssueMutex  sync.Mutex
}

func NewPodIssueService(
	clusterContext executorContext.ClusterContext,
	eventReporter reporter.EventReporter,
	pendingPodChecker podchecks.PodChecker,
	stuckTerminatingPodExpiry time.Duration,
) *PodIssueService {
	podIssueService := &PodIssueService{
		clusterContext:            clusterContext,
		eventReporter:             eventReporter,
		pendingPodChecker:         pendingPodChecker,
		stuckTerminatingPodExpiry: stuckTerminatingPodExpiry,
		knownPodIssues:            map[string]*podIssue{},
		podIssueMutex:             sync.Mutex{},
	}

	clusterContext.AddPodEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			podIssueService.handleDeletedPod(pod)
		},
	})

	return podIssueService
}

func (p *PodIssueService) registerIssue(issue *podIssue) {
	p.podIssueMutex.Lock()
	defer p.podIssueMutex.Unlock()

	runId := issue.RunId
	if runId == "" {
		log.Warnf("Not registering an issue for job %s (%s) as run id was empty", issue.JobId, issue.OriginalPodState.Name)
		return
	}
	_, exists := p.knownPodIssues[issue.RunId]
	if !exists {
		p.knownPodIssues[issue.RunId] = issue
	} else {
		log.Warnf("Not registering an issue for job %s (runId %s) as it already has an issue set", issue.JobId, issue.RunId)
	}
}

func (p *PodIssueService) markIssuesResolved(issue *podIssue) {
	p.podIssueMutex.Lock()
	defer p.podIssueMutex.Unlock()

	delete(p.knownPodIssues, issue.RunId)
}

func (p *PodIssueService) markIssueReported(issue *podIssue) {
	issue.Reported = true
}

func (p *PodIssueService) HandlePodIssues() {
	managedPods, err := p.clusterContext.GetBatchPods()
	if err != nil {
		log.WithError(err).Errorf("unable to handle pod issus as failed to load pods")
	}
	managedPods = util.FilterPods(managedPods, func(pod *v1.Pod) bool {
		return !util.IsLegacyManagedPod(pod)
	})
	p.detectPodIssues(managedPods)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()
	p.handleKnownPodIssues(ctx, managedPods)
}

func (p *PodIssueService) detectPodIssues(allManagedPods []*v1.Pod) {
	for _, pod := range allManagedPods {
		if pod.DeletionTimestamp != nil && pod.DeletionTimestamp.Add(p.stuckTerminatingPodExpiry).Before(time.Now()) {
			// pod is stuck in terminating phase, this sometimes happen on node failure
			// it is safer to produce failed event than retrying as the job might have run already
			issue := &podIssue{
				OriginalPodState: pod.DeepCopy(),
				JobId:            util.ExtractJobId(pod),
				RunId:            util.ExtractJobRunId(pod),
				Message:          "pod stuck in terminating phase, this might be due to platform problems",
				Retryable:        false,
				Type:             StuckTerminating,
			}

			p.registerIssue(issue)
			break
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

			action, cause, podCheckMessage := p.pendingPodChecker.GetAction(pod, podEvents, time.Now().Sub(lastStateChange))

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
					JobId:            util.ExtractJobId(pod),
					RunId:            util.ExtractJobRunId(pod),
					Message:          message,
					Retryable:        retryable,
					Type:             podIssueType,
				}
				p.registerIssue(issue)
				break
			}
		}
	}
}

func (p *PodIssueService) handleKnownPodIssues(ctx context.Context, allManagedPods []*v1.Pod) {
	issues := createIssues(allManagedPods, p.knownPodIssues)
	// Make issues from pods + issues
	util.ProcessItemsWithThreadPool(ctx, 20, issues, p.handlePodIssue)
}

func createIssues(managedPods []*v1.Pod, podIssues map[string]*podIssue) []*issue {
	podsByRunId := make(map[string]*v1.Pod, len(managedPods))

	for _, pod := range managedPods {
		runId := util.ExtractJobRunId(pod)
		if runId != "" {
			podsByRunId[runId] = pod
		} else {
			log.Warnf("failed to find run id for pod %s", pod.Name)
		}
	}

	result := make([]*issue, 0, len(podIssues))

	for _, podIssue := range podIssues {
		relatedPod := podsByRunId[podIssue.RunId]
		result = append(result, &issue{CurrentPodState: relatedPod, Issue: podIssue})
	}

	return result
}

func (p *PodIssueService) handlePodIssue(issue *issue) {
	// Skip jobs with no issues
	if issue == nil {
		return
	}

	hasSelfResolved := hasPodIssueSelfResolved(issue)
	if hasSelfResolved {
		p.markIssuesResolved(issue.Issue)
		return
	}

	if issue.Issue.Retryable {
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
func (p *PodIssueService) handleNonRetryableJobIssue(issue *issue) {
	if !issue.Issue.Reported {
		message := issue.Issue.Message

		events := make([]reporter.EventMessage, 0, 2)
		if issue.Issue.Type == StuckStartingUp || issue.Issue.Type == UnableToSchedule {
			unableToScheduleEvent := reporter.CreateJobUnableToScheduleEvent(issue.Issue.OriginalPodState, message, p.clusterContext.GetClusterId())
			events = append(events, reporter.EventMessage{Event: unableToScheduleEvent, JobRunId: issue.Issue.RunId})
		}
		failedEvent := reporter.CreateSimpleJobFailedEvent(issue.Issue.OriginalPodState, message, p.clusterContext.GetClusterId(), issue.Issue.Cause)
		events = append(events, reporter.EventMessage{Event: failedEvent, JobRunId: issue.Issue.RunId})

		err := p.eventReporter.Report(events)
		if err != nil {
			log.Errorf("Failed to report failed event for job %s because %s", issue.Issue.JobId, err)
			return
		}

		p.markIssueReported(issue.Issue)
	}

	if issue.CurrentPodState != nil {
		p.clusterContext.DeletePods([]*v1.Pod{issue.CurrentPodState})
	} else {
		p.markIssuesResolved(issue.Issue)
	}
}

// For retryable issues we must:
//   - Report JobUnableToScheduleEvent
//   - Report JobReturnLeaseEvent
//
// Special consideration must be taken that most of these pods are somewhat "stuck" in pending.
//
//	So can transition to Running/Completed/Failed in the middle of this
//
// We must not return the lease if the pod state changes - as likely it has become "unstuck"
func (p *PodIssueService) handleRetryableJobIssue(issue *issue) {
	if !issue.Issue.Reported {
		if issue.Issue.Type == StuckStartingUp || issue.Issue.Type == UnableToSchedule {
			event := reporter.CreateJobUnableToScheduleEvent(issue.Issue.OriginalPodState, issue.Issue.Message, p.clusterContext.GetClusterId())
			err := p.eventReporter.Report([]reporter.EventMessage{{Event: event, JobRunId: issue.Issue.RunId}})
			if err != nil {
				log.Errorf("Failure to report stuck pod event %+v because %s", event, err)
				return
			}
		}
		p.markIssueReported(issue.Issue)
	}

	if issue.CurrentPodState != nil {
		// TODO consider moving this to a synchronous call - but long termination periods would need to be handled
		err := p.clusterContext.DeletePodWithCondition(issue.CurrentPodState, func(pod *v1.Pod) bool {
			return pod.Status.Phase == v1.PodPending
		}, true)
		if err != nil {
			log.Errorf("Failed to delete pod of running job %s because %s", issue.Issue.JobId, err)
			return
		}
	}

	jobRunAttempted := issue.Issue.Type != UnableToSchedule
	returnLeaseEvent := reporter.CreateReturnLeaseEvent(issue.Issue.OriginalPodState, issue.Issue.Message, p.clusterContext.GetClusterId(), jobRunAttempted)
	err := p.eventReporter.Report([]reporter.EventMessage{{Event: returnLeaseEvent, JobRunId: issue.Issue.RunId}})
	if err != nil {
		log.Errorf("Failed to return lease for job %s because %s", issue.Issue.JobId, err)
		return
	}
	p.markIssuesResolved(issue.Issue)
}

func hasPodIssueSelfResolved(issue *issue) bool {
	if issue == nil {
		return true
	}

	isStuckStartingUpAndResolvable := issue.Issue.Type == StuckStartingUp &&
		(issue.Issue.Retryable || (!issue.Issue.Retryable && !issue.Issue.Reported))
	if issue.Issue.Type == UnableToSchedule || isStuckStartingUpAndResolvable {
		// If pod has disappeared - don't consider it resolved as we still need to report the issue
		if issue.CurrentPodState == nil {
			return false
		}
		// These are issues causing pods to get stuck in Pending
		// As the pods are no longer Pending, the issue is no longer present
		if issue.CurrentPodState.Status.Phase != v1.PodPending {
			return true
		}
	}

	return false
}

func createStuckPodMessage(retryable bool, originalMessage string) string {
	if retryable {
		return fmt.Sprintf("Unable to schedule pod, Armada will return lease and retry.\n%s", originalMessage)
	}
	return fmt.Sprintf("Unable to schedule pod with unrecoverable problem, Armada will not retry.\n%s", originalMessage)
}

func (p *PodIssueService) handleDeletedPod(pod *v1.Pod) {
	jobId := util.ExtractJobId(pod)
	if jobId != "" {
		isUnexpectedDeletion := !util.IsMarkedForDeletion(pod) && !util.IsPodFinishedAndReported(pod)
		if isUnexpectedDeletion {
			p.registerIssue(&podIssue{
				OriginalPodState: pod,
				JobId:            jobId,
				RunId:            util.ExtractJobRunId(pod),
				Message:          "Pod was unexpected deleted",
				Retryable:        false,
				Reported:         false,
				Type:             ExternallyDeleted,
			})
		}
	}
}
