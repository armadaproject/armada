package job

import (
	"sync"

	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/util"
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
}

type jobRecord struct {
	jobId             string
	issue             *PodIssue
	markedForDeletion bool
}

type JobContext interface {
	GetJobs() ([]*RunningJob, error)
	RegisterIssue(job *RunningJob, issue *PodIssue)
	ResolveIssue(job *RunningJob)
	DeleteJobs(jobs []*RunningJob)
	AddAnnotation(jobs []*RunningJob, annotations map[string]string) error
	IsActiveJob(id string) bool
}

type ClusterJobContext struct {
	clusterContext context.ClusterContext

	activeJobs        map[string]*jobRecord
	activeJobIdsMutex sync.Mutex
}

func NewClusterJobContext(clusterContext context.ClusterContext) *ClusterJobContext {
	jobContext := &ClusterJobContext{
		clusterContext:    clusterContext,
		activeJobs:        map[string]*jobRecord{},
		activeJobIdsMutex: sync.Mutex{},
	}
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

func (c *ClusterJobContext) RegisterIssue(job *RunningJob, issue *PodIssue) {
	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()

	job.Issue = issue

	record, exists := c.activeJobs[job.JobId]
	if exists {
		record.issue = issue
	}
	// TODO what if the job is not registered
}

func (c *ClusterJobContext) ResolveIssue(job *RunningJob) {
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

func (c *ClusterJobContext) IsActiveJob(id string) bool {
	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()

	record, exists := c.activeJobs[id]
	return exists && !record.markedForDeletion
}

func (c *ClusterJobContext) addIssues(jobs []*RunningJob) []*RunningJob {

	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()

	runningJobIds := map[string]*RunningJob{}
	for _, job := range jobs {
		runningJobIds[job.JobId] = job
		_, exists := c.activeJobs[job.JobId]
		if !exists {
			c.activeJobs[job.JobId] = &jobRecord{
				jobId:             job.JobId,
				markedForDeletion: false,
			}
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
