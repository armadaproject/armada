package service

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	context2 "github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/job"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/util"
)

type JobManager struct {
	clusterIdentity context2.ClusterIdentity
	jobContext      job.JobContext
	eventReporter   reporter.EventReporter
	jobLeaseService LeaseService
	minimumPodAge   time.Duration
	failedPodExpiry time.Duration
}

func NewJobManager(
	clusterIdentity context2.ClusterIdentity,
	jobContext job.JobContext,
	eventReporter reporter.EventReporter,
	jobLeaseService LeaseService,
	minimumPodAge time.Duration,
	failedPodExpiry time.Duration) *JobManager {
	return &JobManager{
		clusterIdentity: clusterIdentity,
		jobContext:      jobContext,
		eventReporter:   eventReporter,
		jobLeaseService: jobLeaseService,
		minimumPodAge:   minimumPodAge,
		failedPodExpiry: failedPodExpiry}
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
			m.reportTerminated(extractPods(failedJobs))
			m.jobContext.DeleteJobs(failedJobs)
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

	jobsToCleanup := filterRunningJobs(jobs, m.canBeRemoved)
	m.jobContext.DeleteJobs(jobsToCleanup)

	m.handlePodIssues(jobs)
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
	err := m.jobContext.AddAnnotation(jobs, map[string]string{
		jobDoneAnnotation: time.Now().String(),
	})
	if err != nil {
		log.Warnf("Failed to annotate jobs as done: %v", err)
	}
}

func (m *JobManager) reportTerminated(pods []*v1.Pod) {
	for _, pod := range pods {
		event := reporter.CreateJobTerminatedEvent(pod, "Pod terminated because lease could not be renewed.", m.clusterIdentity.GetClusterId())
		m.eventReporter.QueueEvent(event, func(err error) {
			if err != nil {
				log.Errorf("Failed to report terminated pod %s: %s", pod.Name, err)
			}
		})
	}
}

func (m *JobManager) canBeRemoved(job *job.RunningJob) bool {
	for _, pod := range job.ActivePods {
		if !m.canPodBeRemoved(pod) {
			return false
		}
	}
	return true
}

func (m *JobManager) canPodBeRemoved(pod *v1.Pod) bool {
	if !util.IsInTerminalState(pod) ||
		!isReportedDone(pod) ||
		!reporter.HasCurrentStateBeenReported(pod) {
		return false
	}

	lastContainerStart := util.FindLastContainerStartTime(pod)
	if lastContainerStart.Add(m.minimumPodAge).After(time.Now()) {
		return false
	}

	if pod.Status.Phase == v1.PodFailed {
		lastChange, err := util.LastStatusChange(pod)
		if err == nil && lastChange.Add(m.failedPodExpiry).After(time.Now()) {
			return false
		}
	}
	return true
}

func (m *JobManager) handlePodIssues(allRunningJobs []*job.RunningJob) {

	remainingStuckJobs := []*job.RunningJob{}
	for _, runningJob := range allRunningJobs {
		if runningJob.Issue != nil {
			if !runningJob.Issue.Reported {
				m.reportStuckPods(runningJob)
			}
			if runningJob.Issue.Reported {
				if len(runningJob.ActivePods) == 0 {
					resolved := m.onStuckPodDeleted(runningJob)
					if resolved {
						m.jobContext.MarkIssuesResolved(runningJob)
					}
				} else {
					remainingStuckJobs = append(remainingStuckJobs, runningJob)
				}
			}
		}
	}

	m.reportDoneAndDelete(remainingStuckJobs)
}

func (m *JobManager) reportStuckPods(runningJob *job.RunningJob) {
	if runningJob.Issue == nil || runningJob.Issue.Reported {
		return
	}

	if runningJob.Issue.Type == job.UnableToSchedule {
		event := reporter.CreateJobUnableToScheduleEvent(runningJob.Issue.OriginatingPod, runningJob.Issue.Message, m.clusterIdentity.GetClusterId())
		err := m.eventReporter.Report(event)
		if err != nil {
			log.Errorf("Failure to report stuck pod event %+v because %s", event, err)
		} else {
			m.jobContext.MarkIssueReported(runningJob.Issue)
		}

	} else {
		m.jobContext.MarkIssueReported(runningJob.Issue)
	}
}

func (m *JobManager) reportDoneAndDelete(runningJobs []*job.RunningJob) {

	remainingRetryableJobs := make([]*job.RunningJob, 0, 10)
	remainingNonRetryableJobs := make([]*job.RunningJob, 0, 10)
	remainingNonRetryableJobIds := make([]string, 0, 10)

	for _, record := range runningJobs {
		if record.Issue.Retryable {
			remainingRetryableJobs = append(remainingRetryableJobs, record)
		} else {
			remainingNonRetryableJobs = append(remainingNonRetryableJobs, record)
			remainingNonRetryableJobIds = append(remainingNonRetryableJobIds, record.JobId)
		}
	}

	err := m.jobLeaseService.ReportDone(remainingNonRetryableJobIds)
	if err != nil {
		m.jobContext.DeleteJobs(remainingRetryableJobs)
	} else {
		m.jobContext.DeleteJobs(append(remainingRetryableJobs, remainingNonRetryableJobs...))
	}
}

func (m *JobManager) onStuckPodDeleted(job *job.RunningJob) (resolved bool) {
	// this method is executed after stuck pod was deleted from the cluster
	if job.Issue.Retryable {
		err := m.jobLeaseService.ReturnLease(job.Issue.OriginatingPod)
		if err != nil {
			log.Errorf("Failed to return lease for job %s because %s", job.JobId, err)
			return false
		}

		leaseReturnedEvent := reporter.CreateJobLeaseReturnedEvent(job.Issue.OriginatingPod, job.Issue.Message, m.clusterIdentity.GetClusterId())

		err = m.eventReporter.Report(leaseReturnedEvent)
		if err != nil {
			log.Errorf("Failed to report lease returned for job %s because %s", job.JobId, err)
			// We should fall through to true here, as we have already returned the lease and the event is just for reporting
			// If we fail, we'll try again which could be complicated if the same executor leases is again between retries
		}

	} else {
		// Reporting failed even can fail with unfortunate timing of executor restarts, in that case lease will expire and job can be retried
		// This is preferred over returning Failed event early as user could retry based on failed even but the job could be running
		for _, pod := range job.Issue.Pods {
			message := job.Issue.Message
			if pod.UID != job.Issue.OriginatingPod.UID {
				message = fmt.Sprintf("Peer pod %d stuck.", util.ExtractPodNumber(job.Issue.OriginatingPod))
			}
			event := reporter.CreateSimpleJobFailedEvent(pod, message, m.clusterIdentity.GetClusterId())

			err := m.eventReporter.Report(event)
			if err != nil {
				return false
			}
		}
	}
	return true
}
