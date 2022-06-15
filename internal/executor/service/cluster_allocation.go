package service

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"

	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/healthmonitor"
	"github.com/G-Research/armada/internal/executor/job"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/util"
	"github.com/G-Research/armada/internal/executor/utilisation"
)

type ClusterAllocationService struct {
	leaseService       LeaseService
	eventReporter      reporter.EventReporter
	utilisationService utilisation.UtilisationService
	clusterContext     context.ClusterContext
	submitter          job.Submitter
	etcdHealthMonitor  healthmonitor.EtcdLimitHealthMonitor
}

func NewClusterAllocationService(
	clusterContext context.ClusterContext,
	eventReporter reporter.EventReporter,
	leaseService LeaseService,
	utilisationService utilisation.UtilisationService,
	submitter job.Submitter,
	etcdHealthMonitor healthmonitor.EtcdLimitHealthMonitor) *ClusterAllocationService {

	return &ClusterAllocationService{
		leaseService:       leaseService,
		eventReporter:      eventReporter,
		utilisationService: utilisationService,
		clusterContext:     clusterContext,
		submitter:          submitter,
		etcdHealthMonitor:  etcdHealthMonitor}
}

func (allocationService *ClusterAllocationService) AllocateSpareClusterCapacity() {
	// If a health monitor is provided, avoid leasing jobs when etcd is almost full.
	if allocationService.etcdHealthMonitor != nil && !allocationService.etcdHealthMonitor.IsWithinSoftHealthLimit() {
		log.Warnf("Skipping allocating spare cluster capacity as etcd is at its soft health limit")
		return
	}

	capacityReport, err := allocationService.utilisationService.GetAvailableClusterCapacity()
	if err != nil {
		log.Errorf("Failed to allocate spare cluster capacity because %s", err)
		return
	}

	leasePods, err := allocationService.clusterContext.GetBatchPods()
	if err != nil {
		log.Errorf("Failed to allocate spare cluster capacity because %s", err)
		return
	}
	activePods := util.FilterPods(leasePods, isActive)
	newJobs, err := allocationService.leaseService.RequestJobLeases(capacityReport.AvailableCapacity, capacityReport.Nodes, utilisation.GetAllocationByQueue(activePods))

	cpu := (*capacityReport.AvailableCapacity)["cpu"]
	memory := (*capacityReport.AvailableCapacity)["memory"]
	log.Infof("Requesting new jobs with free resource cpu: %d, memory %d. Received %d new jobs. ", cpu.AsDec(), memory.Value(), len(newJobs))

	if err != nil {
		log.WithError(err).Error("failed to lease new jobs")
		return
	} else {
		failedJobs := allocationService.submitter.SubmitJobs(newJobs)

		err := allocationService.processFailedJobs(failedJobs)
		if err != nil {
			log.Errorf("Failed to process failed jobs  because %s", err)
		}
	}
}

// Any pod not in a terminal state is considered active for the purposes of cluster allocation
// As soon as a pod finishes (enters a terminal state) we should try to allocate another pod
func isActive(pod *v1.Pod) bool {
	return !util.IsInTerminalState(pod)
}

func (allocationService *ClusterAllocationService) processFailedJobs(failedSubmissions []*job.FailedSubmissionDetails) error {
	toBeReportedDone := make([]string, 0, 10)

	for _, details := range failedSubmissions {
		message := details.Error.Error()
		if apiError, ok := details.Error.(errors.APIStatus); ok {
			message = apiError.Status().Message
		}

		if details.Recoverable {
			allocationService.returnLease(details.Pod, fmt.Sprintf("Failed to submit pod because %s", message))
		} else {
			failEvent := reporter.CreateSimpleJobFailedEvent(details.Pod, message, allocationService.clusterContext.GetClusterId())
			err := allocationService.eventReporter.Report(failEvent)

			if err == nil {
				toBeReportedDone = append(toBeReportedDone, details.Job.Id)
			}
		}
	}

	return allocationService.leaseService.ReportDone(toBeReportedDone)
}

func (allocationService *ClusterAllocationService) returnLease(pod *v1.Pod, reason string) {
	err := allocationService.leaseService.ReturnLease(pod)

	if err != nil {
		log.Errorf("Failed to return lease for job %s because %s", util.ExtractJobId(pod), err)
	} else {
		leaseReturnedEvent := reporter.CreateJobLeaseReturnedEvent(pod, reason, allocationService.clusterContext.GetClusterId())

		err = allocationService.eventReporter.Report(leaseReturnedEvent)
		if err != nil {
			log.Errorf("Failed to report event %+v because %s", leaseReturnedEvent, err)
		}
	}
}
