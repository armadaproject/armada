package service

import (
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	context2 "github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/job"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/util"
)

type JobManager struct {
	clusterIdentity  context2.ClusterIdentity
	jobContext       job.JobContext
	eventReporter    reporter.EventReporter
	stuckPodDetector *StuckPodDetector
	jobLeaseService  LeaseService
	minimumPodAge    time.Duration
	failedPodExpiry  time.Duration
}

func NewJobManager(
	clusterIdentity context2.ClusterIdentity,
	jobContext job.JobContext,
	eventReporter reporter.EventReporter,
	stuckPodDetector *StuckPodDetector,
	jobLeaseService LeaseService,
	minimumPodAge time.Duration,
	failedPodExpiry time.Duration) *JobManager {
	return &JobManager{
		clusterIdentity:  clusterIdentity,
		jobContext:       jobContext,
		eventReporter:    eventReporter,
		stuckPodDetector: stuckPodDetector,
		jobLeaseService:  jobLeaseService,
		minimumPodAge:    minimumPodAge,
		failedPodExpiry:  failedPodExpiry}
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
			m.reportTerminated(extractPods(failedJobs))
			m.jobContext.DeleteJobs(failedJobs)
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

	jobsToCleanup := filterRunningJobs(jobs, m.canBeRemoved)
	m.jobContext.DeleteJobs(jobsToCleanup)

	m.stuckPodDetector.HandleStuckPods(jobs)
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
	err := m.jobContext.AddAnnotation(jobs, map[string]string{
		jobDoneAnnotation: time.Now().String(),
	})
	if err != nil {
		log.Warnf("Failed to annotate jobs as done: %v", err)
	}
}

func (m *JobManager) reportTerminated(pods []*v1.Pod) {
	for _, pod := range pods {
		event := reporter.CreateJobTerminatedEvent(pod, "Pod terminated because lease could not be renewed.", m.clusterIdentity.GetClusterId())
		m.eventReporter.QueueEvent(event, func(err error) {
			if err != nil {
				log.Errorf("Failed to report terminated pod %s: %s", pod.Name, err)
			}
		})
	}
}

func (m *JobManager) canBeRemoved(job *job.RunningJob) bool {
	for _, pod := range job.Pods {
		if !m.canPodBeRemoved(pod) {
			return false
		}
	}
	return true
}

func (m *JobManager) canPodBeRemoved(pod *v1.Pod) bool {
	if !util.IsInTerminalState(pod) ||
		!isReportedDone(pod) ||
		!reporter.HasCurrentStateBeenReported(pod) {
		return false
	}

	lastContainerStart := util.FindLastContainerStartTime(pod)
	if lastContainerStart.Add(m.minimumPodAge).After(time.Now()) {
		return false
	}

	if pod.Status.Phase == v1.PodFailed {
		lastChange, err := util.LastStatusChange(pod)
		if err == nil && lastChange.Add(m.failedPodExpiry).After(time.Now()) {
			return false
		}
	}
	return true
}
