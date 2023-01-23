package service

import (
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	commonUtil "github.com/armadaproject/armada/internal/common/util"
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

func (m *JobManager) handlePodIssues(allRunningJobs []*job.RunningJob) {
	m.reportJobsWithIssues(allRunningJobs)
	jobsToDelete := []*job.RunningJob{}
	for _, runningJob := range allRunningJobs {
		if runningJob.Issue != nil {
			if runningJob.Issue.Reported {
				if len(runningJob.ActivePods) == 0 {
					resolved := m.onPodDeleted(runningJob)
					if resolved {
						m.jobContext.MarkIssuesResolved(runningJob)
					}
				} else {
					jobsToDelete = append(jobsToDelete, runningJob)
				}
			}
		}
	}

	m.jobContext.DeleteJobs(jobsToDelete)
}

func (m *JobManager) reportJobsWithIssues(allRunningJobs []*job.RunningJob) {
	jobsToReportDone := filterRunningJobs(allRunningJobs, func(runningJob *job.RunningJob) bool {
		return runningJob.Issue != nil && !runningJob.Issue.Reported && !runningJob.Issue.Retryable
	})

	jobsFailedToBeReportedDone := map[string]bool{}
	err := m.jobLeaseService.ReportDone(extractJobIds(jobsToReportDone))
	if err != nil {
		log.Errorf("Failed to report jobs %s done because %s", strings.Join(extractJobIds(jobsToReportDone), ","), err)
		jobsFailedToBeReportedDone = commonUtil.StringListToSet(extractJobIds(jobsToReportDone))
	}

	for _, runningJob := range allRunningJobs {
		// Skip already reported
		if runningJob.Issue == nil || runningJob.Issue.Reported {
			continue
		}

		// Skip those that failed to be reported done
		if _, present := jobsFailedToBeReportedDone[runningJob.JobId]; present {
			continue
		}

		if runningJob.Issue.Type == job.StuckStartingUp || runningJob.Issue.Type == job.UnableToSchedule {
			event := reporter.CreateJobUnableToScheduleEvent(runningJob.Issue.OriginatingPod, runningJob.Issue.Message, m.clusterIdentity.GetClusterId())
			err := m.eventReporter.Report(reporter.EventMessage{Event: event, JobRunId: util.ExtractJobRunId(runningJob.Issue.OriginatingPod)})
			if err != nil {
				log.Errorf("Failure to report stuck pod event %+v because %s", event, err)
			} else {
				m.jobContext.MarkIssueReported(runningJob.Issue)
			}

		} else {
			m.jobContext.MarkIssueReported(runningJob.Issue)
		}
	}
}

// onPodDeleted handles cases when either a stuck pod was deleted or a pod was preempted
func (m *JobManager) onPodDeleted(runningJob *job.RunningJob) (resolved bool) {
	// this method is executed after stuck pod was deleted from the cluster
	if runningJob.Issue.Retryable {
		jobRunAttempted := runningJob.Issue.Type != job.UnableToSchedule
		err := m.jobLeaseService.ReturnLease(runningJob.Issue.OriginatingPod, runningJob.Issue.Message, jobRunAttempted)
		if err != nil {
			log.Errorf("Failed to return lease for job %s because %s", runningJob.JobId, err)
			return false
		}
	} else {
		// Reporting failed even can fail with unfortunate timing of executor restarts, in that case lease will expire and job can be retried
		// This is preferred over returning Failed event early as user could retry based on failed even but the job could be running
		for _, pod := range runningJob.Issue.Pods {
			message := runningJob.Issue.Message
			if pod.UID != runningJob.Issue.OriginatingPod.UID {
				message = fmt.Sprintf("Peer pod %d stuck.", util.ExtractPodNumber(runningJob.Issue.OriginatingPod))
			}
			event := reporter.CreateSimpleJobFailedEvent(pod, message, m.clusterIdentity.GetClusterId(), runningJob.Issue.Cause)

			err := m.eventReporter.Report(reporter.EventMessage{Event: event, JobRunId: util.ExtractJobRunId(pod)})
			if err != nil {
				return false
			}
		}
	}
	return true
}
