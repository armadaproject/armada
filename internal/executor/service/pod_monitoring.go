package service

import (
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	listers "k8s.io/client-go/listers/core/v1"

	"github.com/G-Research/k8s-batch/internal/armada/api"
	commonUtil "github.com/G-Research/k8s-batch/internal/common/util"
	"github.com/G-Research/k8s-batch/internal/executor/reporter"
	"github.com/G-Research/k8s-batch/internal/executor/util"
)

type PodProgressMonitor interface {
	HandleStuckPods()
}

type PodProgressMonitorService struct {
	podLister       listers.PodLister
	eventReporter   reporter.EventReporter
	stuckPodCache   util.PodCache
	cleanupService  PodCleanupService
	jobLeaseService JobLeaseService
	clusterId       string
}

func NewPodProgressMonitorService(podLister listers.PodLister, eventReporter reporter.EventReporter, cleanupService PodCleanupService,
	jobLeaseService JobLeaseService, clusterId string) *PodProgressMonitorService {
	return &PodProgressMonitorService{
		podLister:       podLister,
		eventReporter:   eventReporter,
		stuckPodCache:   util.NewMapPodCache("stuck_job"),
		cleanupService:  cleanupService,
		jobLeaseService: jobLeaseService,
		clusterId:       clusterId,
	}
}

func (podProgressMonitor *PodProgressMonitorService) HandleStuckPods() {
	allBatchPods, err := podProgressMonitor.podLister.List(util.GetManagedPodSelector())
	if err != nil {
		log.Errorf("Failed to reconcile missing job events because %s", err)
		return
	}

	for _, pod := range allBatchPods {
		jobId := util.ExtractJobId(pod)
		if podProgressMonitor.stuckPodCache.Get(jobId) != nil {
			continue
		}

		if (pod.Status.Phase == v1.PodUnknown || pod.Status.Phase == v1.PodPending) && hasPodBeenInStateForLongerThanGivenDuration(pod, 30*time.Second) {
			err := podProgressMonitor.reportStuckPodEvent(pod)
			if err == nil {
				podProgressMonitor.stuckPodCache.AddIfNotExists(pod)
			}
		}
	}
	podProgressMonitor.processStuckPodCache(allBatchPods)
}

func (podProgressMonitor *PodProgressMonitorService) reportStuckPodEvent(pod *v1.Pod) error {
	var event api.Event

	if util.IsRetryable(pod) {
		event = util.CreateJobUnableToScheduleEvent(pod, util.ExtractPodStuckReason(pod), podProgressMonitor.clusterId)
	} else {
		event = util.CreateJobFailedEvent(pod, util.ExtractPodStuckReason(pod), podProgressMonitor.clusterId)
	}

	return podProgressMonitor.eventReporter.Report(event)
}

func (podProgressMonitor *PodProgressMonitorService) processStuckPodCache(existingPods []*v1.Pod) {
	jobIds := util.ExtractJobIds(existingPods)
	jobIdSet := commonUtil.StringListToSet(jobIds)

	remainingStuckPods := make([]*v1.Pod, 0, 10)

	for _, pod := range podProgressMonitor.stuckPodCache.GetAll() {
		jobId := util.ExtractJobId(pod)
		if _, exists := jobIdSet[jobId]; !exists {
			if util.IsRetryable(pod) {
				err := podProgressMonitor.jobLeaseService.ReturnLease(pod)
				if err != nil {
					log.Errorf("Failed to return lease for job %s because %s", jobId, err)
					continue
				}
			}
			podProgressMonitor.stuckPodCache.Delete(jobId)
		} else {
			remainingStuckPods = append(remainingStuckPods, pod)
		}
	}

	podProgressMonitor.markStuckPodsForDeletion(remainingStuckPods)
}

func (podProgressMonitor *PodProgressMonitorService) markStuckPodsForDeletion(pods []*v1.Pod) {
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
		podProgressMonitor.cleanupService.DeletePods(remainingRetryablePods)
	} else {
		podProgressMonitor.cleanupService.DeletePods(append(remainingRetryablePods, remainingNonRetryablePods...))
	}
}
