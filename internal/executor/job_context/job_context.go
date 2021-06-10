package job_context

import (
	"sync"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/util"
)

type RunningJob struct {
	JobId string
	Pods  []*v1.Pod
}

type JobContext interface {
	GetRunningJobs() ([]*RunningJob, error)
	DeleteJobs(jobs []*RunningJob)
	RegisterActiveJobs(ids []string)
	RegisterDoneJobs(ids []string)
	IsActiveJob(id string) bool
}

type ClusterJobContext struct {
	clusterContext context.ClusterContext

	activeJobIds      map[string]bool
	activeJobIdsMutex sync.Mutex
}

func NewClusterJobContext(clusterContext context.ClusterContext) *ClusterJobContext {
	return &ClusterJobContext{
		clusterContext:    clusterContext,
		activeJobIds:      map[string]bool{},
		activeJobIdsMutex: sync.Mutex{},
	}
}

func (c *ClusterJobContext) GetRunningJobs() ([]*RunningJob, error) {
	pods, err := c.clusterContext.GetActiveBatchPods()
	if err != nil {
		return nil, err
	}
	return groupRunningJobs(pods), nil
}

func (c *ClusterJobContext) DeleteJobs(jobs []*RunningJob) {
	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()

	for _, job := range jobs {
		delete(c.activeJobIds, job.JobId)
		log.Infof("Delete job id %s via JobContext", job.JobId)
		c.clusterContext.DeletePods(job.Pods)
	}
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

	return c.activeJobIds[id]
}

func (c *ClusterJobContext) RegisterActiveJobs(ids []string) {
	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()

	for _, id := range ids {
		c.activeJobIds[id] = true
	}
}

func (c *ClusterJobContext) RegisterDoneJobs(ids []string) {
	c.activeJobIdsMutex.Lock()
	defer c.activeJobIdsMutex.Unlock()

	for _, id := range ids {
		delete(c.activeJobIds, id)
	}
}
