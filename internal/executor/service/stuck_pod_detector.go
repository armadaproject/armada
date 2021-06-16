package service

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/job"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/util"
)

type StuckPodDetector struct {
	clusterContext  context.ClusterContext
	jobContext      job.JobContext
	eventReporter   reporter.EventReporter
	jobLeaseService LeaseService
	stuckPodExpiry  time.Duration
}

func NewPodProgressMonitorService(
	clusterContext context.ClusterContext,
	jobContext job.JobContext,
	eventReporter reporter.EventReporter,
	jobLeaseService LeaseService,
	stuckPodExpiry time.Duration) *StuckPodDetector {

	return &StuckPodDetector{
		clusterContext:  clusterContext,
		jobContext:      jobContext,
		eventReporter:   eventReporter,
		jobLeaseService: jobLeaseService,
		stuckPodExpiry:  stuckPodExpiry,
	}
}

func (d *StuckPodDetector) HandlePodIssues(allRunningJobs []*job.RunningJob) {

	remainingStuckJobs := []*job.RunningJob{}
	for _, runningJob := range allRunningJobs {
		if runningJob.Issue != nil {
			if !runningJob.Issue.Reported {
				d.reportStuckPods(runningJob)
			}
			if runningJob.Issue.Reported {
				if len(runningJob.Pods) == 0 {
					resolved := d.onStuckPodDeleted(runningJob)
					if resolved {
						d.jobContext.ResolveIssues(runningJob)
					}
				} else {
					remainingStuckJobs = append(remainingStuckJobs, runningJob)
				}
			}
		}
	}

	d.reportDoneAndDelete(remainingStuckJobs)
}

func (d *StuckPodDetector) reportStuckPods(runningJob *job.RunningJob) {
	if runningJob.Issue == nil || runningJob.Issue.Reported {
		return
	}

	if runningJob.Issue.Type == job.UnableToSchedule {
		event := reporter.CreateJobUnableToScheduleEvent(runningJob.Issue.OriginatingPod, runningJob.Issue.Message, d.clusterContext.GetClusterId())
		err := d.eventReporter.Report(event)
		if err != nil {
			log.Errorf("Failure to report stuck pod event %+v because %s", event, err)
		} else {
			runningJob.Issue.Reported = true
		}

	} else {
		runningJob.Issue.Reported = true
	}
}

func (d *StuckPodDetector) reportDoneAndDelete(runningJobs []*job.RunningJob) {

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

	err := d.jobLeaseService.ReportDone(remainingNonRetryableJobIds)
	if err != nil {
		d.jobContext.DeleteJobs(remainingRetryableJobs)
	} else {
		d.jobContext.DeleteJobs(append(remainingRetryableJobs, remainingNonRetryableJobs...))
	}
}

func (d *StuckPodDetector) onStuckPodDeleted(job *job.RunningJob) (resolved bool) {
	// this method is executed after stuck pod was deleted from the cluster
	if job.Issue.Retryable {
		err := d.jobLeaseService.ReturnLease(job.Issue.OriginatingPod)
		if err != nil {
			log.Errorf("Failed to return lease for job %s because %s", job.JobId, err)
			return false
		}

		leaseReturnedEvent := reporter.CreateJobLeaseReturnedEvent(job.Issue.OriginatingPod, job.Issue.Message, d.clusterContext.GetClusterId())

		err = d.eventReporter.Report(leaseReturnedEvent)
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
			event := reporter.CreateSimpleJobFailedEvent(pod, message, d.clusterContext.GetClusterId())

			err := d.eventReporter.Report(event)
			if err != nil {
				return false
			}
		}
	}

	return true
}
