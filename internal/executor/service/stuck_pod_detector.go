package service

import (
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/armada/api"
	commonUtil "github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/util"
)

type StuckPodDetector struct {
	clusterContext  context.ClusterContext
	eventReporter   reporter.EventReporter
	stuckPodCache   map[string]*v1.Pod
	jobLeaseService LeaseService
	clusterId       string
}

func NewPodProgressMonitorService(
	clusterContext context.ClusterContext,
	eventReporter reporter.EventReporter,
	jobLeaseService LeaseService) *StuckPodDetector {

	return &StuckPodDetector{
		clusterContext:  clusterContext,
		eventReporter:   eventReporter,
		stuckPodCache:   map[string]*v1.Pod{},
		jobLeaseService: jobLeaseService,
	}
}

func (podProgressMonitor *StuckPodDetector) onStuckPodDetected(pod *v1.Pod) (resolved bool) {
	var event api.Event

	if util.IsRetryable(pod) {
		event = reporter.CreateJobUnableToScheduleEvent(pod, util.ExtractPodStuckReason(pod), podProgressMonitor.clusterId)
	} else {
		event = reporter.CreateJobFailedEvent(pod, util.ExtractPodStuckReason(pod), podProgressMonitor.clusterId)
	}

	err := podProgressMonitor.eventReporter.Report(event)
	if err != nil {
		log.Errorf("Failure to stuck pod event %+v because %s", event, err)
	}
	return err == nil
}

func (podProgressMonitor *StuckPodDetector) onStuckPodDeleted(jobId string, pod *v1.Pod) (resolved bool) {
	if util.IsRetryable(pod) {
		err := podProgressMonitor.jobLeaseService.ReturnLease(pod)
		if err != nil {
			log.Errorf("Failed to return lease for job %s because %s", jobId, err)
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

			resolved := podProgressMonitor.onStuckPodDetected(pod)
			if resolved {
				podProgressMonitor.stuckPodCache[jobId] = pod.DeepCopy()
			}
		}
	}
	podProgressMonitor.processStuckPodCache(allBatchPods)
}

func (podProgressMonitor *StuckPodDetector) processStuckPodCache(existingPods []*v1.Pod) {
	jobIds := util.ExtractJobIds(existingPods)
	jobIdSet := commonUtil.StringListToSet(jobIds)

	remainingStuckPods := make([]*v1.Pod, 0, 10)

	cacheCopy := make([]*v1.Pod, 0, len(podProgressMonitor.stuckPodCache))

	for _, pod := range podProgressMonitor.stuckPodCache {
		cacheCopy = append(cacheCopy, pod)
	}

	for _, pod := range cacheCopy {
		jobId := util.ExtractJobId(pod)
		if _, exists := jobIdSet[jobId]; !exists {
			resolved := podProgressMonitor.onStuckPodDeleted(jobId, pod)
			if !resolved {
				continue
			}
			delete(podProgressMonitor.stuckPodCache, jobId)
		} else {
			remainingStuckPods = append(remainingStuckPods, pod)
		}
	}

	podProgressMonitor.markStuckPodsForDeletion(remainingStuckPods)
}

func (podProgressMonitor *StuckPodDetector) markStuckPodsForDeletion(pods []*v1.Pod) {
	remainingRetryablePods := make([]*v1.Pod, 0, 10)
	remainingNonRetryablePods := make([]*v1.Pod, 0, 10)

	for _, pod := range pods {
		if util.IsRetryable(pod) {
			remainingRetryablePods = append(remainingRetryablePods, pod)
		} else {
			remainingNonRetryablePods = append(remainingNonRetryablePods, pod)
		}
	}

	err := podProgressMonitor.jobLeaseService.ReportDone(remainingNonRetryablePods)
	if err != nil {
		podProgressMonitor.clusterContext.DeletePods(remainingRetryablePods)
	} else {
		podProgressMonitor.clusterContext.DeletePods(append(remainingRetryablePods, remainingNonRetryablePods...))
	}
}
