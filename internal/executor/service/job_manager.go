package service

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	context2 "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/util"
)

const maxPodRequestSize = 10000

type JobManager struct {
	clusterIdentity context2.ClusterIdentity
	jobContext      job.JobContext
	eventReporter   reporter.EventReporter
	jobLeaseService LeaseService
}

func NewJobManager(
	clusterIdentity context2.ClusterIdentity,
	jobContext job.JobContext,
	eventReporter reporter.EventReporter,
	jobLeaseService LeaseService,
) *JobManager {
	return &JobManager{
		clusterIdentity: clusterIdentity,
		jobContext:      jobContext,
		eventReporter:   eventReporter,
		jobLeaseService: jobLeaseService,
	}
}

func (m *JobManager) ManageJobLeases() {
	jobs, err := m.jobContext.GetJobs()
	if err != nil {
		log.Errorf("Failed to manage job leases due to %s", err)
		return
	}

	jobsToRenew := filterRunningJobs(jobs, jobShouldBeRenewed)
	chunkedJobs := chunkJobs(jobsToRenew, maxPodRequestSize)
	for _, chunk := range chunkedJobs {
		failedJobs, err := m.jobLeaseService.RenewJobLeases(chunk)
		if err == nil && len(failedJobs) > 0 {
			// This happens in case of lease being revoked - normally due to cancellation
			// In which case, we should delete the job
			jobsToDelete := filterRunningJobs(failedJobs, func(runningJob *job.RunningJob) bool {
				for _, pod := range runningJob.ActivePods {
					if !util.IsInTerminalState(pod) {
						return true
					}
				}
				return false
			})
			m.reportTerminated(extractPods(jobsToDelete))
			m.jobContext.DeleteJobs(jobsToDelete)
		}
	}

	jobsForReporting := filterRunningJobs(jobs, shouldBeReportedDone)
	chunkedJobsToReportDone := chunkJobs(jobsForReporting, maxPodRequestSize)
	for _, chunk := range chunkedJobsToReportDone {
		err = m.reportDoneAndMarkReported(chunk)
		if err != nil {
			log.Errorf("Failed reporting jobs as done because %s", err)
			return
		}
	}

	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), time.Minute*2)
	defer cancel()
	m.handlePodIssues(ctx, jobs)
}

func (m *JobManager) reportDoneAndMarkReported(jobs []*job.RunningJob) error {
	if len(jobs) <= 0 {
		return nil
	}
	err := m.jobLeaseService.ReportDone(extractJobIds(jobs))
	if err == nil {
		m.markAsDone(jobs)
	}
	return err
}

func (m *JobManager) markAsDone(jobs []*job.RunningJob) {
	m.jobContext.AddAnnotation(jobs, map[string]string{
		domain.JobDoneAnnotation: time.Now().String(),
	})
}

func (m *JobManager) reportTerminated(pods []*v1.Pod) {
	for _, pod := range pods {
		event := reporter.CreateJobTerminatedEvent(pod, "Pod terminated because lease could not be renewed.", m.clusterIdentity.GetClusterId())
		m.eventReporter.QueueEvent(reporter.EventMessage{Event: event, JobRunId: util.ExtractJobRunId(pod)}, func(err error) {
			if err != nil {
				log.Errorf("Failed to report terminated pod %s: %s", pod.Name, err)
			}
		})
	}
}

func (m *JobManager) handlePodIssues(ctx *armadacontext.ArmadaContext, allRunningJobs []*job.RunningJob) {
	util.ProcessItemsWithThreadPool(ctx, 20, allRunningJobs, m.handlePodIssue)
}

func (m *JobManager) handlePodIssue(runningJob *job.RunningJob) {
	// Skip jobs with no issues
	if runningJob.Issue == nil {
		return
	}

	hasSelfResolved := hasIssueSelfResolved(runningJob)
	if hasSelfResolved {
		m.jobContext.MarkIssuesResolved(runningJob)
		return
	}

	if runningJob.Issue.Retryable {
		m.handleRetryableJobIssue(runningJob)
	} else {
		m.handleNonRetryableJobIssue(runningJob)
	}
}

// For non-retryable issues we must:
//   - Report JobUnableToScheduleEvent if the issue is a startup issue
//   - Report JobFailedEvent
//   - Report the job done
//
// Once that is done we are free to cleanup the pod
func (m *JobManager) handleNonRetryableJobIssue(runningJob *job.RunningJob) {
	if !runningJob.Issue.Reported {
		for _, pod := range runningJob.Issue.Pods {
			message := runningJob.Issue.Message
			if pod.UID != runningJob.Issue.OriginatingPod.UID {
				message = fmt.Sprintf("Peer pod %d stuck.", util.ExtractPodNumber(runningJob.Issue.OriginatingPod))
			}
			events := make([]reporter.EventMessage, 0, 2)
			if runningJob.Issue.Type == job.StuckStartingUp || runningJob.Issue.Type == job.UnableToSchedule {
				unableToScheduleEvent := reporter.CreateJobUnableToScheduleEvent(pod, message, m.clusterIdentity.GetClusterId())
				events = append(events, reporter.EventMessage{Event: unableToScheduleEvent, JobRunId: util.ExtractJobRunId(pod)})
			}
			failedEvent := reporter.CreateSimpleJobFailedEvent(pod, message, m.clusterIdentity.GetClusterId(), runningJob.Issue.Cause)
			events = append(events, reporter.EventMessage{Event: failedEvent, JobRunId: util.ExtractJobRunId(pod)})

			err := m.eventReporter.Report(events)
			if err != nil {
				log.Errorf("Failed to report failed event for job %s because %s", runningJob.JobId, err)
				return
			}
		}

		err := m.jobLeaseService.ReportDone([]string{runningJob.JobId})
		if err != nil {
			log.Errorf("Failed to report job %s done because %s", runningJob.JobId, err)
			return
		}
		log.Infof("Non-retryable issue detected for job %s - %s", runningJob.JobId, runningJob.Issue.Message)
		m.jobContext.MarkIssueReported(runningJob.Issue)
	}

	if len(runningJob.ActivePods) > 0 {
		m.jobContext.DeleteJobs([]*job.RunningJob{runningJob})
	} else {
		m.jobContext.MarkIssuesResolved(runningJob)
	}
}

// For retryable issues we must:
//   - Report JobUnableToScheduleEvent
//   - Return job lease
//
// Special consideration must be taken that most of these pods are somewhat "stuck" in pending.
//
//	So can transition to Running/Completed/Failed in the middle of this
//
// We must not return the lease if the pod state changes - as likely it has become "unstuck"
func (m *JobManager) handleRetryableJobIssue(runningJob *job.RunningJob) {
	if !runningJob.Issue.Reported {
		if runningJob.Issue.Type == job.StuckStartingUp || runningJob.Issue.Type == job.UnableToSchedule {
			unableToScheduleEvent := reporter.CreateJobUnableToScheduleEvent(runningJob.Issue.OriginatingPod, runningJob.Issue.Message, m.clusterIdentity.GetClusterId())
			err := m.eventReporter.Report([]reporter.EventMessage{{Event: unableToScheduleEvent, JobRunId: util.ExtractJobRunId(runningJob.Issue.OriginatingPod)}})
			if err != nil {
				log.Errorf("Failure to report stuck pod event %+v because %s", unableToScheduleEvent, err)
				return
			}
		}
		log.Infof("Retryable issue detected for job %s - %s", runningJob.JobId, runningJob.Issue.Message)
		m.jobContext.MarkIssueReported(runningJob.Issue)
	}

	if len(runningJob.ActivePods) > 0 {
		// TODO consider moving this to a synchronous call - but long termination periods would need to be handled
		err := m.jobContext.DeleteJobWithCondition(runningJob, func(pod *v1.Pod) bool {
			return pod.Status.Phase == v1.PodPending
		})
		if err != nil {
			log.Errorf("Failed to delete pod of running job %s because %s", runningJob.JobId, err)
			return
		}
	} else {
		jobRunAttempted := runningJob.Issue.Type != job.UnableToSchedule
		err := m.jobLeaseService.ReturnLease(runningJob.Issue.OriginatingPod, runningJob.Issue.Message, jobRunAttempted)
		if err != nil {
			log.Errorf("Failed to return lease for job %s because %s", runningJob.JobId, err)
			return
		}
		m.jobContext.MarkIssuesResolved(runningJob)
	}
}

func hasIssueSelfResolved(runningJob *job.RunningJob) bool {
	if runningJob.Issue == nil {
		return true
	}

	isStuckStartingUpAndResolvable := runningJob.Issue.Type == job.StuckStartingUp &&
		(runningJob.Issue.Retryable || (!runningJob.Issue.Retryable && !runningJob.Issue.Reported))
	if runningJob.Issue.Type == job.UnableToSchedule || isStuckStartingUpAndResolvable {
		for _, pod := range runningJob.ActivePods {
			// Pod has completed - no need to report any issues
			if util.IsInTerminalState(pod) {
				return true
			}

			// Pod has started running, and we haven't requested deletion - let it continue
			if pod.Status.Phase == v1.PodRunning && pod.DeletionTimestamp == nil {
				return true
			}
		}
	}

	return false
}
