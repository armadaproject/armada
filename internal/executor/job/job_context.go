package job

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/util"
)

type IssueType int

const (
	UnableToSchedule  IssueType = iota
	StuckTerminating  IssueType = iota
	ExternallyDeleted IssueType = iota
)

type RunningJob struct {
	JobId string
	Pods  []*v1.Pod
	Issue *PodIssue
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
	jobId             string
	issue             *PodIssue
	markedForDeletion bool
	externallyDeleted bool
}

type JobContext interface {
	GetJobs() ([]*RunningJob, error)
	ResolveIssues(job *RunningJob)
	DeleteJobs(jobs []*RunningJob)
	AddAnnotation(jobs []*RunningJob, annotations map[string]string) error
	IsActiveJob(id string) bool
}

type ClusterJobContext struct {
	clusterContext context.ClusterContext
	stuckPodExpiry time.Duration

	activeJobs        map[string]*jobRecord
	activeJobIdsMutex sync.Mutex
}

func NewClusterJobContext(clusterContext context.ClusterContext) *ClusterJobContext {
	jobContext := &ClusterJobContext{
		clusterContext:    clusterContext,
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

func (c *ClusterJobContext) ResolveIssues(job *RunningJob) {
	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()

	record, exists := c.activeJobs[job.JobId]
	if exists {
		record.issue = nil
	}
}

func (c *ClusterJobContext) DeleteJobs(jobs []*RunningJob) {
	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()

	for _, job := range jobs {
		record, exists := c.activeJobs[job.JobId]
		if !exists {
			c.activeJobs[job.JobId] = &jobRecord{
				jobId:             job.JobId,
				markedForDeletion: true,
			}
		} else {
			record.markedForDeletion = true
		}
		c.clusterContext.DeletePods(job.Pods)
	}
}

func (c *ClusterJobContext) AddAnnotation(jobs []*RunningJob, annotations map[string]string) error {
	for _, job := range jobs {
		for _, pod := range job.Pods {
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
			JobId: jobId,
			Pods:  pods,
		})
	}
	return result
}

func (c *ClusterJobContext) registerIssue(job *RunningJob, issue *PodIssue) {
	job.Issue = issue

	record, exists := c.activeJobs[job.JobId]
	if exists {
		record.issue = issue
	}
	// TODO what if the job is not registered
}

func (c *ClusterJobContext) IsActiveJob(id string) bool {
	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()

	record, exists := c.activeJobs[id]
	return exists && !record.markedForDeletion
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
				jobId:             job.JobId,
				markedForDeletion: false,
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
					JobId: jobId,
					Pods:  nil,
					Issue: record.issue,
				})
			} else {
				delete(c.activeJobs, jobId)
			}
		}
	}
	return jobs
}

func (c *ClusterJobContext) detectStuckPods(runningJob *RunningJob) {
	for _, pod := range runningJob.Pods {
		if pod.DeletionTimestamp != nil && pod.DeletionTimestamp.Add(c.stuckPodExpiry).Before(time.Now()) {
			// pod is stuck in terminating phase, this sometimes happen on node failure
			// its safer to produce failed event than retrying as the job might have run already
			c.registerIssue(runningJob, &PodIssue{
				OriginatingPod: pod.DeepCopy(),
				Pods:           runningJob.Pods,
				Message:        "pod stuck in terminating phase, this might be due to platform problems",
				Retryable:      false,
				Type:           StuckTerminating})

		} else if (pod.Status.Phase == v1.PodUnknown || pod.Status.Phase == v1.PodPending) &&
			reporter.HasPodBeenInStateForLongerThanGivenDuration(pod, c.stuckPodExpiry) {

			podEvents, err := c.clusterContext.GetPodEvents(pod)
			if err != nil {
				log.Errorf("Unable to get pod events: %v", err)
			}

			retryable, message := util.DiagnoseStuckPod(pod, podEvents)
			if retryable {
				message = fmt.Sprintf("Unable to schedule pod, Armada will return lease and retry.\n%s", message)
			} else {
				message = fmt.Sprintf("Unable to schedule pod with unrecoverable problem, Armada will not retry.\n%s", message)
			}
			c.registerIssue(runningJob, &PodIssue{
				OriginatingPod: pod.DeepCopy(),
				Pods:           runningJob.Pods,
				Message:        message,
				Retryable:      retryable,
				Type:           UnableToSchedule,
			})
		}
	}
}

func (c *ClusterJobContext) handleDeletedPod(pod *v1.Pod) {
	jobId := util.ExtractJobId(pod)
	if jobId != "" && c.IsActiveJob(jobId) {
		record, exists := c.activeJobs[jobId]
		active := exists && !record.markedForDeletion
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
