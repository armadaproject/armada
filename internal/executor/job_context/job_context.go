package job_context

import (
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
}

type ClusterJobContext struct {
	clusterContext context.ClusterContext
}

func (c ClusterJobContext) GetRunningJobs() ([]*RunningJob, error) {
	pods, err := c.clusterContext.GetActiveBatchPods()
	if err != nil {
		return nil, err
	}
	return groupRunningJobs(pods), nil
}

func (c ClusterJobContext) DeleteJobs(jobs []*RunningJob) {
	for _, job := range jobs {
		c.clusterContext.DeletePods(job.Pods)
	}
}

func NewClusterJobContext(clusterContext context.ClusterContext) *ClusterJobContext {
	return &ClusterJobContext{clusterContext: clusterContext}
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
