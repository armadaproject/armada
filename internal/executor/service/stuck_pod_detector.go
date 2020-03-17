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
}

type podRecord struct {
	pod       *v1.Pod
	retryable bool
}

func NewPodProgressMonitorService(
	clusterContext context.ClusterContext,
	eventReporter reporter.EventReporter,
	jobLeaseService LeaseService) *StuckPodDetector {

	return &StuckPodDetector{
		clusterContext:  clusterContext,
		eventReporter:   eventReporter,
		stuckPodCache:   map[string]*podRecord{},
		jobLeaseService: jobLeaseService,
	}
}

func (d *StuckPodDetector) onStuckPodDetected(pod *v1.Pod) (err error, retryable bool) {

	podEvents, err := d.clusterContext.GetPodEvents(pod)
	if err != nil {
		log.Errorf("Unable to get pod events: %v", err)
	}

	retryable, message := util.DiagnoseStuckPod(pod, podEvents)
	if retryable {
		message = fmt.Sprintf("Unable to schedule pod, Armada will retrun lease and retry.\n%s", message)
	} else {
		message = fmt.Sprintf("Unable to schedule pod with unrecoverable problem, Armada will not retry.\n%s", message)
	}
	event := reporter.CreateJobUnableToScheduleEvent(pod, message, d.clusterId)
	err = d.eventReporter.Report(event)
	if err != nil {
		log.Errorf("Failure to stuck pod event %+v because %s", event, err)
	}
	return err, retryable
}

func (podProgressMonitor *StuckPodDetector) onStuckPodDeleted(jobId string, record *podRecord) (resolved bool) {
	if record.retryable {
		err := podProgressMonitor.jobLeaseService.ReturnLease(record.pod)
		if err != nil {
			log.Errorf("Failed to return lease for job %s because %s", jobId, err)
			return false
		}

		leaseReturnedEvent := reporter.CreateJobLeaseReturnedEvent(record.pod, util.ExtractPodStuckReason(record.pod), podProgressMonitor.clusterContext.GetClusterId())

		err = podProgressMonitor.eventReporter.Report(leaseReturnedEvent)
		if err != nil {
			log.Errorf("Failed to report lease returned for job %s because %s", jobId, err)
			// We should fall through to true here, as we have already returned the lease and the event is just for reporting
			// If we fail, we'll try again which could be complicated if the same executor leases is again between retries
		}

	} else {
		// Reporting failed even can fail with unfortunate timing of executor restarts, in that case lease will expire and job can be retried
		// This is preferred over returning Failed event early as user could retry based on failed even but the job could be running
		event := reporter.CreateJobFailedEvent(record.pod, util.ExtractPodStuckReason(record.pod), map[string]int32{}, podProgressMonitor.clusterId)
		err := podProgressMonitor.eventReporter.Report(event)
		if err != nil {
			return false
		}
	}

	return true
}

func (podProgressMonitor *StuckPodDetector) HandleStuckPods() {
	allBatchPods, err := podProgressMonitor.clusterContext.GetActiveBatchPods()
	if err != nil {
		log.Errorf("Failed to load all pods for stuck pod handling %s ", err)
		return
	}

	for _, pod := range allBatchPods {
		jobId := util.ExtractJobId(pod)
		_, exists := podProgressMonitor.stuckPodCache[jobId]

		if exists {
			continue
		}
		if (pod.Status.Phase == v1.PodUnknown || pod.Status.Phase == v1.PodPending) && reporter.HasPodBeenInStateForLongerThanGivenDuration(pod, 5*time.Minute) {
			err, retryable := podProgressMonitor.onStuckPodDetected(pod)
			if err == nil {
				podProgressMonitor.stuckPodCache[jobId] = &podRecord{pod.DeepCopy(), retryable}
			}
		}
	}
	podProgressMonitor.processStuckPodCache(allBatchPods)
}

func (podProgressMonitor *StuckPodDetector) processStuckPodCache(existingPods []*v1.Pod) {
	jobIds := util.ExtractJobIds(existingPods)
	jobIdSet := commonUtil.StringListToSet(jobIds)

	remainingStuckPods := make([]*podRecord, 0, 10)

	cacheCopy := make([]*podRecord, 0, len(podProgressMonitor.stuckPodCache))

	for _, record := range podProgressMonitor.stuckPodCache {
		cacheCopy = append(cacheCopy, record)
	}

	for _, record := range cacheCopy {
		jobId := util.ExtractJobId(record.pod)
		if _, exists := jobIdSet[jobId]; !exists {
			resolved := podProgressMonitor.onStuckPodDeleted(jobId, record)
			if !resolved {
				continue
			}
			delete(podProgressMonitor.stuckPodCache, jobId)
		} else {
			remainingStuckPods = append(remainingStuckPods, record)
		}
	}

	podProgressMonitor.markStuckPodsForDeletion(remainingStuckPods)
}

func (podProgressMonitor *StuckPodDetector) markStuckPodsForDeletion(records []*podRecord) {
	remainingRetryablePods := make([]*v1.Pod, 0, 10)
	remainingNonRetryablePods := make([]*v1.Pod, 0, 10)

	for _, record := range records {
		if record.retryable {
			remainingRetryablePods = append(remainingRetryablePods, record.pod)
		} else {
			remainingNonRetryablePods = append(remainingNonRetryablePods, record.pod)
		}
	}

	err := podProgressMonitor.jobLeaseService.ReportDone(remainingNonRetryablePods)
	if err != nil {
		podProgressMonitor.clusterContext.DeletePods(remainingRetryablePods)
	} else {
		podProgressMonitor.clusterContext.DeletePods(append(remainingRetryablePods, remainingNonRetryablePods...))
	}
}
