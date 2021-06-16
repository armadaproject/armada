package service

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

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

	detector := &StuckPodDetector{
		clusterContext:  clusterContext,
		jobContext:      jobContext,
		eventReporter:   eventReporter,
		jobLeaseService: jobLeaseService,
		stuckPodExpiry:  stuckPodExpiry,
	}

	clusterContext.AddPodEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			detector.handleDeletedPod(pod)
		},
	})

	return detector
}

func (d *StuckPodDetector) HandleStuckPods(allRunningJobs []*job.RunningJob) {

	remainingStuckJobs := []*job.RunningJob{}

	for _, runningJob := range allRunningJobs {
		if runningJob.Issue == nil {
			d.registerAndReportStuckPods(runningJob)
		}
	}

	for _, runningJob := range allRunningJobs {
		if runningJob.Issue != nil {
			if len(runningJob.Pods) == 0 {
				resolved := d.onStuckPodDeleted(runningJob)
				if resolved {
					d.jobContext.ResolveIssue(runningJob)
				}
			} else {
				remainingStuckJobs = append(remainingStuckJobs, runningJob)
			}
		}
	}

	d.reportDoneAndDelete(remainingStuckJobs)
}

func (d *StuckPodDetector) registerAndReportStuckPods(runningJob *job.RunningJob) {
	for _, pod := range runningJob.Pods {
		if pod.DeletionTimestamp != nil && pod.DeletionTimestamp.Add(d.stuckPodExpiry).Before(time.Now()) {
			// pod is stuck in terminating phase, this sometimes happen on node failure
			// its safer to produce failed event than retrying as the job might have run already
			d.jobContext.RegisterIssue(runningJob, &job.PodIssue{
				OriginatingPod: pod.DeepCopy(),
				Pods:           runningJob.Pods,
				Message:        "pod stuck in terminating phase, this might be due to platform problems",
				Retryable:      false})

		} else if (pod.Status.Phase == v1.PodUnknown || pod.Status.Phase == v1.PodPending) &&
			reporter.HasPodBeenInStateForLongerThanGivenDuration(pod, d.stuckPodExpiry) {
			err, retryable, message := d.determineStuckPodState(pod)
			if err == nil {
				d.jobContext.RegisterIssue(runningJob, &job.PodIssue{
					OriginatingPod: pod.DeepCopy(),
					Pods:           runningJob.Pods,
					Message:        message,
					Retryable:      retryable})
			}
		}
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

func (d *StuckPodDetector) handleDeletedPod(pod *v1.Pod) {
	// fail jobs which had their pod deleted externally
	jobId := util.ExtractJobId(pod)
	if jobId != "" && d.jobContext.IsActiveJob(jobId) {
		err := d.jobLeaseService.ReportDone([]string{jobId})
		if err == nil {
			err := d.eventReporter.Report(reporter.CreateSimpleJobFailedEvent(pod, "Pod of the active job was deleted.", d.clusterContext.GetClusterId()))
			if err != nil {
				log.Errorf("Failed to report externally deleted job finished %s", err)
			}
		} else {
			log.Errorf("Failed to report externally deleted job done %s", err)
		}
	}
}
