package service

import (
	"context"
	"strings"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common"
	commonUtil "github.com/G-Research/armada/internal/common/util"
	context2 "github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/job_context"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/util"
	"github.com/G-Research/armada/pkg/api"
)

const maxPodRequestSize = 10000
const jobDoneAnnotation = "reported_done"

type LeaseService interface {
	ReturnLease(pod *v1.Pod) error
	RequestJobLeases(availableResource *common.ComputeResources, nodes []api.NodeInfo, leasedResourceByQueue map[string]common.ComputeResources) ([]*api.Job, error)
	ReportDone(jobIds []string) error
}

type JobLeaseService struct {
	clusterContext  context2.ClusterContext
	jobContext      job_context.JobContext
	eventReporter   reporter.EventReporter
	queueClient     api.AggregatedQueueClient
	minimumPodAge   time.Duration
	failedPodExpiry time.Duration
	minimumJobSize  common.ComputeResources
}

func NewJobLeaseService(
	clusterContext context2.ClusterContext,
	jobContext job_context.JobContext,
	eventReporter reporter.EventReporter,
	queueClient api.AggregatedQueueClient,
	minimumPodAge time.Duration,
	failedPodExpiry time.Duration,
	minimumJobSize common.ComputeResources) *JobLeaseService {

	return &JobLeaseService{
		clusterContext:  clusterContext,
		jobContext:      jobContext,
		eventReporter:   eventReporter,
		queueClient:     queueClient,
		minimumPodAge:   minimumPodAge,
		failedPodExpiry: failedPodExpiry,
		minimumJobSize:  minimumJobSize}
}

func (jobLeaseService *JobLeaseService) RequestJobLeases(availableResource *common.ComputeResources, nodes []api.NodeInfo, leasedResourceByQueue map[string]common.ComputeResources) ([]*api.Job, error) {
	leasedQueueReports := make([]*api.QueueLeasedReport, 0, len(leasedResourceByQueue))
	for queueName, leasedResource := range leasedResourceByQueue {
		leasedQueueReport := &api.QueueLeasedReport{
			Name:            queueName,
			ResourcesLeased: leasedResource,
		}
		leasedQueueReports = append(leasedQueueReports, leasedQueueReport)
	}
	clusterLeasedReport := api.ClusterLeasedReport{
		ClusterId:  jobLeaseService.clusterContext.GetClusterId(),
		ReportTime: time.Now(),
		Queues:     leasedQueueReports,
	}

	leaseRequest := api.LeaseRequest{
		ClusterId:           jobLeaseService.clusterContext.GetClusterId(),
		Pool:                jobLeaseService.clusterContext.GetClusterPool(),
		Resources:           *availableResource,
		ClusterLeasedReport: clusterLeasedReport,
		Nodes:               nodes,
		MinimumJobSize:      jobLeaseService.minimumJobSize,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := jobLeaseService.queueClient.LeaseJobs(ctx, &leaseRequest, grpc_retry.WithMax(1))

	if err != nil {
		return make([]*api.Job, 0), err
	}

	return response.Job, nil
}

func (jobLeaseService *JobLeaseService) ReturnLease(pod *v1.Pod) error {
	jobId := util.ExtractJobId(pod)
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	log.Infof("Returning lease for job %s", jobId)
	_, err := jobLeaseService.queueClient.ReturnLease(ctx, &api.ReturnLeaseRequest{ClusterId: jobLeaseService.clusterContext.GetClusterId(), JobId: jobId})

	jobLeaseService.jobContext.RegisterDoneJobs([]string{jobId})

	return err
}

func (jobLeaseService *JobLeaseService) ManageJobLeases() {
	jobs, err := jobLeaseService.jobContext.GetRunningJobs()
	if err != nil {
		log.Errorf("Failed to manage job leases due to %s", err)
		return
	}

	jobsToRenew := filterRunningJobs(jobs, jobShouldBeRenewed)
	chunkedJobs := chunkJobs(jobsToRenew, maxPodRequestSize)
	for _, chunk := range chunkedJobs {
		jobLeaseService.renewJobLeases(chunk)
	}

	jobsForReporting := filterRunningJobs(jobs, shouldBeReportedDone)
	chunkedJobsToReportDone := chunkJobs(jobsForReporting, maxPodRequestSize)
	for _, chunk := range chunkedJobsToReportDone {
		err = jobLeaseService.reportDoneAndMarkReported(chunk)
		if err != nil {
			log.Errorf("Failed reporting jobs as done because %s", err)
			return
		}
	}

	podsToCleanup := util.FilterPods(extractPods(jobs), jobLeaseService.canBeRemoved)
	jobLeaseService.clusterContext.DeletePods(podsToCleanup)
}

func (jobLeaseService *JobLeaseService) reportDoneAndMarkReported(jobs []*job_context.RunningJob) error {
	if len(jobs) <= 0 {
		return nil
	}
	err := jobLeaseService.ReportDone(extractJobIds(jobs))
	if err == nil {
		jobLeaseService.markAsDone(extractPods(jobs))
	}
	return err
}

func (jobLeaseService *JobLeaseService) ReportDone(jobIds []string) error {
	if len(jobIds) <= 0 {
		return nil
	}
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	log.Infof("Reporting done for jobs %s", strings.Join(jobIds, ","))
	_, err := jobLeaseService.queueClient.ReportDone(ctx, &api.IdList{Ids: jobIds})

	jobLeaseService.jobContext.RegisterDoneJobs(jobIds)

	return err
}

func extractJobIds(jobs []*job_context.RunningJob) []string {
	ids := []string{}
	for _, job := range jobs {
		ids = append(ids, job.JobId)
	}
	return ids
}

func extractPods(jobs []*job_context.RunningJob) []*v1.Pod {
	pods := []*v1.Pod{}
	for _, job := range jobs {
		pods = append(pods, job.Pods...)
	}
	return pods
}

func (jobLeaseService *JobLeaseService) renewJobLeases(jobs []*job_context.RunningJob) {
	if len(jobs) <= 0 {
		return
	}
	jobIds := extractJobIds(jobs)
	log.Infof("Renewing lease for %s", strings.Join(jobIds, ","))

	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	renewedJobIds, err := jobLeaseService.queueClient.RenewLease(ctx,
		&api.RenewLeaseRequest{
			ClusterId: jobLeaseService.clusterContext.GetClusterId(),
			Ids:       jobIds})
	if err != nil {
		log.Errorf("Failed to renew lease for jobs because %s", err)
		return
	}

	failedIds := commonUtil.SubtractStringList(jobIds, renewedJobIds.Ids)
	failedPods := filterPodsByJobId(extractPods(jobs), failedIds)

	jobLeaseService.jobContext.RegisterActiveJobs(renewedJobIds.Ids)
	jobLeaseService.jobContext.RegisterDoneJobs(failedIds)

	if len(failedIds) > 0 {
		log.Warnf("Server has prevented renewing of job lease for jobs %s", strings.Join(failedIds, ","))
		jobLeaseService.reportTerminated(failedPods)
		jobLeaseService.clusterContext.DeletePods(failedPods)
	}
}

func (jobLeaseService *JobLeaseService) markAsDone(pods []*v1.Pod) {
	for _, pod := range pods {
		err := jobLeaseService.clusterContext.AddAnnotation(pod, map[string]string{
			jobDoneAnnotation: time.Now().String(),
		})
		if err != nil {
			log.Warnf("Failed to annotate pod as done: %v", err)
		}
	}
}

func (jobLeaseService *JobLeaseService) reportTerminated(pods []*v1.Pod) {
	for _, pod := range pods {
		event := reporter.CreateJobTerminatedEvent(pod, "Pod terminated because lease could not be renewed.", jobLeaseService.clusterContext.GetClusterId())
		jobLeaseService.eventReporter.QueueEvent(event, func(err error) {
			if err != nil {
				log.Errorf("Failed to report terminated pod %s: %s", pod.Name, err)
			}
		})
	}
}

func shouldBeRenewed(pod *v1.Pod) bool {
	return !isReportedDone(pod)
}

func jobShouldBeRenewed(job *job_context.RunningJob) bool {
	for _, pod := range job.Pods {
		if !isReportedDone(pod) {
			return true
		}
	}
	return false
}

func shouldBeReportedDone(job *job_context.RunningJob) bool {
	for _, pod := range job.Pods {
		if util.IsInTerminalState(pod) && !isReportedDone(pod) {
			return true
		}
	}
	return false
}

func (jobLeaseService *JobLeaseService) canBeRemoved(pod *v1.Pod) bool {
	if !util.IsInTerminalState(pod) ||
		!isReportedDone(pod) ||
		!reporter.HasCurrentStateBeenReported(pod) {
		return false
	}

	lastContainerStart := util.FindLastContainerStartTime(pod)
	if lastContainerStart.Add(jobLeaseService.minimumPodAge).After(time.Now()) {
		return false
	}

	if pod.Status.Phase == v1.PodFailed {
		lastChange, err := util.LastStatusChange(pod)
		if err == nil && lastChange.Add(jobLeaseService.failedPodExpiry).After(time.Now()) {
			return false
		}
	}
	return true
}

func isReportedDone(pod *v1.Pod) bool {
	_, exists := pod.Annotations[jobDoneAnnotation]
	return exists
}

func filterPodsByJobId(pods []*v1.Pod, ids []string) []*v1.Pod {
	reportedIdSet := commonUtil.StringListToSet(ids)
	filteredPods := make([]*v1.Pod, 0)
	for _, pod := range pods {
		if reportedIdSet[util.ExtractJobId(pod)] {
			filteredPods = append(filteredPods, pod)
		}
	}
	return filteredPods
}

func chunkJobs(jobs []*job_context.RunningJob, size int) [][]*job_context.RunningJob {
	chunks := [][]*job_context.RunningJob{}
	for start := 0; start < len(jobs); start += size {
		end := start + size
		if end > len(jobs) {
			end = len(jobs)
		}
		chunks = append(chunks, jobs[start:end])
	}
	return chunks
}

func filterRunningJobs(jobs []*job_context.RunningJob, filter func(*job_context.RunningJob) bool) []*job_context.RunningJob {
	result := make([]*job_context.RunningJob, 0)
	for _, job := range jobs {
		if filter(job) {
			result = append(result, job)
		}
	}
	return result
}
