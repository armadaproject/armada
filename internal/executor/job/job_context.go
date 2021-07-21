package job

import (
	"fmt"
	"sync"
	"time"

	"github.com/G-Research/armada/internal/executor/service"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/util"
)

const defaultTimeBeforeCheckingPendingPodHealth = time.Second * 90

type IssueType int

const (
	UnableToSchedule  IssueType = iota
	StuckTerminating  IssueType = iota
	ExternallyDeleted IssueType = iota
)

type RunningJob struct {
	JobId      string
	ActivePods []*v1.Pod
	Issue      *PodIssue
}

type PodIssue struct {
	OriginatingPod *v1.Pod
	Pods           []*v1.Pod
	Message        string
	Retryable      bool
	Reported       bool
	Type           IssueType
}

type jobRecord struct {
	jobId string
	issue *PodIssue
}

type JobContext interface {
	GetJobs() ([]*RunningJob, error)
	MarkIssueReported(issue *PodIssue)
	MarkIssuesResolved(job *RunningJob)
	DeleteJobs(jobs []*RunningJob)
	AddAnnotation(jobs []*RunningJob, annotations map[string]string) error
}

type ClusterJobContext struct {
	clusterContext context.ClusterContext
	stuckPodExpiry time.Duration

	activeJobs        map[string]*jobRecord
	activeJobIdsMutex sync.Mutex
}

func NewClusterJobContext(clusterContext context.ClusterContext, stuckPodExpiry time.Duration) *ClusterJobContext {
	jobContext := &ClusterJobContext{
		clusterContext:    clusterContext,
		stuckPodExpiry:    stuckPodExpiry,
		activeJobs:        map[string]*jobRecord{},
		activeJobIdsMutex: sync.Mutex{},
	}

	clusterContext.AddPodEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			jobContext.handleDeletedPod(pod)
		},
	})

	return jobContext
}

func (c *ClusterJobContext) GetJobs() ([]*RunningJob, error) {
	pods, err := c.clusterContext.GetActiveBatchPods()
	if err != nil {
		return nil, err
	}

	runningJobs := groupRunningJobs(pods)
	return c.addIssues(runningJobs), nil
}

func (c *ClusterJobContext) MarkIssuesResolved(job *RunningJob) {
	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()

	record, exists := c.activeJobs[job.JobId]
	if exists {
		record.issue = nil
	}
}

func (c *ClusterJobContext) MarkIssueReported(issue *PodIssue) {
	issue.Reported = true
}

func (c *ClusterJobContext) DeleteJobs(jobs []*RunningJob) {
	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()

	for _, job := range jobs {
		c.clusterContext.DeletePods(job.ActivePods)
	}
}

func (c *ClusterJobContext) AddAnnotation(jobs []*RunningJob, annotations map[string]string) error {
	for _, job := range jobs {
		for _, pod := range job.ActivePods {
			err := c.clusterContext.AddAnnotation(pod, annotations)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func groupRunningJobs(pods []*v1.Pod) []*RunningJob {
	podsByJobId := map[string][]*v1.Pod{}
	for _, pod := range pods {
		jobId := util.ExtractJobId(pod)
		podsByJobId[jobId] = append(podsByJobId[jobId], pod)
	}
	result := []*RunningJob{}
	for jobId, pods := range podsByJobId {
		result = append(result, &RunningJob{
			JobId:      jobId,
			ActivePods: pods,
		})
	}
	return result
}

func (c *ClusterJobContext) registerIssue(job *RunningJob, issue *PodIssue) {
	job.Issue = issue

	record, exists := c.activeJobs[job.JobId]
	if exists {
		record.issue = issue
	} else {
		log.Errorf("Resolving issue without existing record (jobId: %s)", job.JobId)
	}
}

func (c *ClusterJobContext) addIssues(jobs []*RunningJob) []*RunningJob {

	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()

	// register new jobs
	runningJobIds := map[string]*RunningJob{}
	for _, job := range jobs {
		runningJobIds[job.JobId] = job
		record, exists := c.activeJobs[job.JobId]
		if !exists {
			record = &jobRecord{
				jobId: job.JobId,
			}
			c.activeJobs[job.JobId] = record
		}
		if record.issue == nil {
			c.detectStuckPods(job)
		}
	}

	for jobId, record := range c.activeJobs {
		runningJob, isRunning := runningJobIds[jobId]
		if isRunning {
			runningJob.Issue = record.issue
		} else {
			if record.issue != nil {
				jobs = append(jobs, &RunningJob{
					JobId:      jobId,
					ActivePods: nil,
					Issue:      record.issue,
				})
			} else {
				delete(c.activeJobs, jobId)
			}
		}
	}
	return jobs
}

func (c *ClusterJobContext) detectStuckPods(runningJob *RunningJob) {
	gracePeriodBeforeHealthCheck := defaultTimeBeforeCheckingPendingPodHealth
	if gracePeriodBeforeHealthCheck > c.stuckPodExpiry {
		gracePeriodBeforeHealthCheck = c.stuckPodExpiry
	}

	for _, pod := range runningJob.ActivePods {
		if pod.DeletionTimestamp != nil && pod.DeletionTimestamp.Add(c.stuckPodExpiry).Before(time.Now()) {
			// pod is stuck in terminating phase, this sometimes happen on node failure
			// its safer to produce failed event than retrying as the job might have run already
			c.registerIssue(runningJob, &PodIssue{
				OriginatingPod: pod.DeepCopy(),
				Pods:           runningJob.ActivePods,
				Message:        "pod stuck in terminating phase, this might be due to platform problems",
				Retryable:      false,
				Type:           StuckTerminating})
			break

		} else if (pod.Status.Phase == v1.PodUnknown || pod.Status.Phase == v1.PodPending) &&
			reporter.HasPodBeenInStateForLongerThanGivenDuration(pod, gracePeriodBeforeHealthCheck) {

			podEvents, err := c.clusterContext.GetPodEvents(pod)
			if err != nil {
				log.Errorf("Unable to get pod events: %v", err)
			}

			stuckPodStatus, message := util.DiagnoseStuckPod(pod, podEvents)
			retryable := stuckPodStatus == util.Healthy

			if stuckPodStatus != util.Unrecoverable && !reporter.HasPodBeenInStateForLongerThanGivenDuration(pod, c.stuckPodExpiry) {
				// Possibly stuck, but don't do anything until expiry is up
				continue
			}

			message = createStuckPodMessage(retryable, message)
			c.registerIssue(runningJob, &PodIssue{
				OriginatingPod: pod.DeepCopy(),
				Pods:           runningJob.ActivePods,
				Message:        message,
				Retryable:      retryable,
				Type:           UnableToSchedule,
			})
			break
		}
	}
}

func createStuckPodMessage(retryable bool, originalMessage string) string {
	if retryable {
		return fmt.Sprintf("Unable to schedule pod, Armada will return lease and retry.\n%s", originalMessage)
	}
	return fmt.Sprintf("Unable to schedule pod with unrecoverable problem, Armada will not retry.\n%s", originalMessage)
}

func (c *ClusterJobContext) handleDeletedPod(pod *v1.Pod) {
	jobId := util.ExtractJobId(pod)
	if jobId != "" {
		record, exists := c.activeJobs[jobId]
		active := exists && !util.IsMarkedForDeletion(pod) && !service.IsPodFinishedAndReported(pod)
		if active {
			record.issue = &PodIssue{
				OriginatingPod: pod,
				Pods:           []*v1.Pod{pod},
				Message:        "Pod of the active job was deleted.",
				Retryable:      false,
				Reported:       false,
				Type:           ExternallyDeleted,
			}
		}
	}
}
