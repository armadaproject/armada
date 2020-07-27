package service

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	commonUtil "github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/util"
)

type StuckPodDetector struct {
	clusterContext  context.ClusterContext
	eventReporter   reporter.EventReporter
	stuckPodCache   map[string]*podRecord
	jobLeaseService LeaseService
	clusterId       string
	stuckPodExpiry  time.Duration
}

type podRecord struct {
	pod       *v1.Pod
	message   string
	retryable bool
}

func NewPodProgressMonitorService(
	clusterContext context.ClusterContext,
	eventReporter reporter.EventReporter,
	jobLeaseService LeaseService,
	stuckPodExpiry time.Duration) *StuckPodDetector {

	return &StuckPodDetector{
		clusterContext:  clusterContext,
		eventReporter:   eventReporter,
		stuckPodCache:   map[string]*podRecord{},
		jobLeaseService: jobLeaseService,
		stuckPodExpiry:  stuckPodExpiry,
	}
}

func (d *StuckPodDetector) onStuckPodDetected(pod *v1.Pod) (err error, retryable bool, message string) {

	podEvents, err := d.clusterContext.GetPodEvents(pod)
	if err != nil {
		log.Errorf("Unable to get pod events: %v", err)
	}

	retryable, message = util.DiagnoseStuckPod(pod, podEvents)
	if retryable {
		message = fmt.Sprintf("Unable to schedule pod, Armada will retrun lease and retry.\n%s", message)
	} else {
		message = fmt.Sprintf("Unable to schedule pod with unrecoverable problem, Armada will not retry.\n%s", message)
	}
	event := reporter.CreateJobUnableToScheduleEvent(pod, message, d.clusterId)
	err = d.eventReporter.Report(event)
	if err != nil {
		log.Errorf("Failure to report stuck pod event %+v because %s", event, err)
	}
	return err, retryable, message
}

func (d *StuckPodDetector) onStuckPodDeleted(jobId string, record *podRecord) (resolved bool) {
	if record.retryable {
		err := d.jobLeaseService.ReturnLease(record.pod)
		if err != nil {
			log.Errorf("Failed to return lease for job %s because %s", jobId, err)
			return false
		}

		leaseReturnedEvent := reporter.CreateJobLeaseReturnedEvent(record.pod, record.message, d.clusterContext.GetClusterId())

		err = d.eventReporter.Report(leaseReturnedEvent)
		if err != nil {
			log.Errorf("Failed to report lease returned for job %s because %s", jobId, err)
			// We should fall through to true here, as we have already returned the lease and the event is just for reporting
			// If we fail, we'll try again which could be complicated if the same executor leases is again between retries
		}

	} else {
		// Reporting failed even can fail with unfortunate timing of executor restarts, in that case lease will expire and job can be retried
		// This is preferred over returning Failed event early as user could retry based on failed even but the job could be running
		event := reporter.CreateJobFailedEvent(record.pod, record.message, map[string]int32{}, d.clusterId)
		err := d.eventReporter.Report(event)
		if err != nil {
			return false
		}
	}

	return true
}

func (d *StuckPodDetector) HandleStuckPods() {
	allBatchPods, err := d.clusterContext.GetActiveBatchPods()
	if err != nil {
		log.Errorf("Failed to load all pods for stuck pod handling %s ", err)
		return
	}

	for _, pod := range allBatchPods {
		jobId := util.ExtractJobId(pod)
		_, exists := d.stuckPodCache[jobId]

		if exists {
			continue
		}
		if (pod.Status.Phase == v1.PodUnknown || pod.Status.Phase == v1.PodPending) && reporter.HasPodBeenInStateForLongerThanGivenDuration(pod, d.stuckPodExpiry) {
			err, retryable, message := d.onStuckPodDetected(pod)
			if err == nil {
				d.stuckPodCache[jobId] = &podRecord{pod.DeepCopy(), message, retryable}
			}
		}
	}
	d.processStuckPodCache(allBatchPods)
}

func (d *StuckPodDetector) processStuckPodCache(existingPods []*v1.Pod) {
	jobIds := util.ExtractJobIds(existingPods)
	jobIdSet := commonUtil.StringListToSet(jobIds)

	remainingStuckPods := make([]*podRecord, 0, 10)

	cacheCopy := make([]*podRecord, 0, len(d.stuckPodCache))

	for _, record := range d.stuckPodCache {
		cacheCopy = append(cacheCopy, record)
	}

	for _, record := range cacheCopy {
		jobId := util.ExtractJobId(record.pod)
		if _, exists := jobIdSet[jobId]; !exists {
			resolved := d.onStuckPodDeleted(jobId, record)
			if !resolved {
				continue
			}
			delete(d.stuckPodCache, jobId)
		} else {
			remainingStuckPods = append(remainingStuckPods, record)
		}
	}

	d.markStuckPodsForDeletion(remainingStuckPods)
}

func (d *StuckPodDetector) markStuckPodsForDeletion(records []*podRecord) {
	remainingRetryablePods := make([]*v1.Pod, 0, 10)
	remainingNonRetryablePods := make([]*v1.Pod, 0, 10)

	for _, record := range records {
		if record.retryable {
			remainingRetryablePods = append(remainingRetryablePods, record.pod)
		} else {
			remainingNonRetryablePods = append(remainingNonRetryablePods, record.pod)
		}
	}

	err := d.jobLeaseService.ReportDone(remainingNonRetryablePods)
	if err != nil {
		d.clusterContext.DeletePods(remainingRetryablePods)
	} else {
		d.clusterContext.DeletePods(append(remainingRetryablePods, remainingNonRetryablePods...))
	}
}
