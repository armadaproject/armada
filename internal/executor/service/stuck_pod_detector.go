package service

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/job_context"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/util"
)

type StuckPodDetector struct {
	clusterContext  context.ClusterContext
	jobContext      job_context.JobContext
	eventReporter   reporter.EventReporter
	stuckJobCache   map[string]*stuckJobRecord
	jobLeaseService LeaseService
	stuckPodExpiry  time.Duration
}

type stuckJobRecord struct {
	job       *job_context.RunningJob
	pod       *v1.Pod
	message   string
	retryable bool
}

func NewPodProgressMonitorService(
	clusterContext context.ClusterContext,
	jobContext job_context.JobContext,
	eventReporter reporter.EventReporter,
	jobLeaseService LeaseService,
	stuckPodExpiry time.Duration) *StuckPodDetector {

	return &StuckPodDetector{
		clusterContext:  clusterContext,
		jobContext:      jobContext,
		eventReporter:   eventReporter,
		stuckJobCache:   map[string]*stuckJobRecord{},
		jobLeaseService: jobLeaseService,
		stuckPodExpiry:  stuckPodExpiry,
	}
}

func (d *StuckPodDetector) determineStuckPodState(pod *v1.Pod) (err error, retryable bool, message string) {

	podEvents, err := d.clusterContext.GetPodEvents(pod)
	if err != nil {
		log.Errorf("Unable to get pod events: %v", err)
	}

	retryable, message = util.DiagnoseStuckPod(pod, podEvents)
	if retryable {
		message = fmt.Sprintf("Unable to schedule pod, Armada will return lease and retry.\n%s", message)
	} else {
		message = fmt.Sprintf("Unable to schedule pod with unrecoverable problem, Armada will not retry.\n%s", message)
	}
	event := reporter.CreateJobUnableToScheduleEvent(pod, message, d.clusterContext.GetClusterId())
	err = d.eventReporter.Report(event)
	if err != nil {
		log.Errorf("Failure to report stuck pod event %+v because %s", event, err)
	}
	return err, retryable, message
}

func (d *StuckPodDetector) onStuckPodDeleted(record *stuckJobRecord) (resolved bool) {
	// this method is executed after stuck pod was deleted from the cluster
	if record.retryable {
		err := d.jobLeaseService.ReturnLease(record.pod)
		if err != nil {
			log.Errorf("Failed to return lease for job %s because %s", record.job.JobId, err)
			return false
		}

		leaseReturnedEvent := reporter.CreateJobLeaseReturnedEvent(record.pod, record.message, d.clusterContext.GetClusterId())

		err = d.eventReporter.Report(leaseReturnedEvent)
		if err != nil {
			log.Errorf("Failed to report lease returned for job %s because %s", record.job.JobId, err)
			// We should fall through to true here, as we have already returned the lease and the event is just for reporting
			// If we fail, we'll try again which could be complicated if the same executor leases is again between retries
		}

	} else {
		// Reporting failed even can fail with unfortunate timing of executor restarts, in that case lease will expire and job can be retried
		// This is preferred over returning Failed event early as user could retry based on failed even but the job could be running
		for _, pod := range record.job.Pods {
			message := record.message
			if pod.UID != record.pod.UID {
				message = fmt.Sprintf("Peer pod %d stuck.", util.ExtractPodNumber(record.pod))
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

func (d *StuckPodDetector) HandleStuckPods() {
	allRunningJobs, err := d.jobContext.GetRunningJobs()
	if err != nil {
		log.Errorf("Failed to load all pods for stuck pod handling %s ", err)
		return
	}

	for _, job := range allRunningJobs {
		_, exists := d.stuckJobCache[job.JobId]
		if exists {
			continue
		}

		for _, pod := range job.Pods {
			if pod.DeletionTimestamp != nil && pod.DeletionTimestamp.Add(d.stuckPodExpiry).Before(time.Now()) {
				// pod is stuck in terminating phase, this sometimes happen on node failure
				// its safer to produce failed event than retrying as the job might have run already
				d.stuckJobCache[job.JobId] = &stuckJobRecord{
					job:       job,
					pod:       pod.DeepCopy(),
					message:   "pod stuck in terminating phase, this might be due to platform problems",
					retryable: false}

			} else if (pod.Status.Phase == v1.PodUnknown || pod.Status.Phase == v1.PodPending) &&
				reporter.HasPodBeenInStateForLongerThanGivenDuration(pod, d.stuckPodExpiry) {

				err, retryable, message := d.determineStuckPodState(pod)
				if err == nil {
					d.stuckJobCache[job.JobId] = &stuckJobRecord{
						job:       job,
						pod:       pod.DeepCopy(),
						message:   message,
						retryable: retryable}
				}
			}
		}
	}
	d.processStuckPodCache(allRunningJobs)
}

func (d *StuckPodDetector) processStuckPodCache(existingJobs []*job_context.RunningJob) {
	jobIdSet := map[string]bool{}
	for _, job := range existingJobs {
		jobIdSet[job.JobId] = true
	}

	cacheCopy := make([]*stuckJobRecord, 0, len(d.stuckJobCache))
	for _, record := range d.stuckJobCache {
		cacheCopy = append(cacheCopy, record)
	}

	remainingStuckPods := make([]*stuckJobRecord, 0, 10)

	for _, record := range cacheCopy {
		if _, exists := jobIdSet[record.job.JobId]; !exists {
			// returns lease here
			resolved := d.onStuckPodDeleted(record)
			if resolved {
				delete(d.stuckJobCache, record.job.JobId)
			}
		} else {
			remainingStuckPods = append(remainingStuckPods, record)
		}
	}
	// calls report done
	d.markStuckPodsForDeletion(remainingStuckPods)
}

func (d *StuckPodDetector) markStuckPodsForDeletion(records []*stuckJobRecord) {
	remainingRetryableJobs := make([]*job_context.RunningJob, 0, 10)
	remainingNonRetryableJobs := make([]*job_context.RunningJob, 0, 10)
	remainingNonRetryableJobIds := make([]string, 0, 10)

	for _, record := range records {
		if record.retryable {
			remainingRetryableJobs = append(remainingRetryableJobs, record.job)
		} else {
			remainingNonRetryableJobs = append(remainingNonRetryableJobs, record.job)
			remainingNonRetryableJobIds = append(remainingNonRetryableJobIds, record.job.JobId)
		}
	}

	err := d.jobLeaseService.ReportDone(remainingNonRetryableJobIds)
	if err != nil {
		d.jobContext.DeleteJobs(remainingRetryableJobs)
	} else {
		d.jobContext.DeleteJobs(append(remainingRetryableJobs, remainingNonRetryableJobs...))
	}
}
