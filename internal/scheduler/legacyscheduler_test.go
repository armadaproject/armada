package scheduler

import (
	"context"
	"fmt"
	"testing"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/G-Research/armada/pkg/api"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func testSchedule(
	nodes []*schedulerobjects.Node,
	jobsByQueue map[string][]*api.Job,
	initialUsageByQueue map[string]schedulerobjects.QuantityByPriorityAndResourceType,
	scheduler *LegacyScheduler,
	expectedScheduledJobsByQueue map[string][]string,
	t *testing.T) {

	nodeDb, err := NewNodeDb(testPriorities, testResources)
	if !assert.NoError(t, err) {
		return
	}
	err = nodeDb.Upsert(nodes)
	if !assert.NoError(t, err) {
		return
	}
	scheduler.NodeDb = nodeDb

	jobQueue := newFakeJobQueue()
	jobQueue.jobsByQueue = jobsByQueue
	scheduler.JobQueue = jobQueue

	jobs, err := scheduler.Schedule(context.Background(), initialUsageByQueue)
	if !assert.NoError(t, err) {
		return
	}

	actualScheduledJobsByQueue := jobIdsByQueueFromJobs(jobs)
	assert.Equal(t, expectedScheduledJobsByQueue, actualScheduledJobsByQueue)
}

func TestScheduleOne2(t *testing.T) {
	nodes := []*schedulerobjects.Node{
		testCpuNode(testPriorities),
	}
	scheduler := &LegacyScheduler{
		SchedulingConfig: configuration.SchedulingConfig{},
		ExecutorId:       "executor",
		MinimumJobSize:   make(map[string]resource.Quantity),
	}

	queue := "queue"
	jobsByQueue := map[string][]*api.Job{
		queue: {
			apiJobFromPodSpec(queue, podSpecFromPodRequirements(testSmallCpuJob())),
		},
	}
	expectedScheduledJobs := jobIdsByQueueFromJobs(jobsByQueue[queue])

	initialUsageByQueue := map[string]schedulerobjects.QuantityByPriorityAndResourceType{
		queue: {
			0: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu": resource.MustParse("1"),
				},
			},
		},
	}

	testSchedule(nodes, jobsByQueue, initialUsageByQueue, scheduler, expectedScheduledJobs, t)
}

func TestScheduleOneQueue(t *testing.T) {
	nodes := []*schedulerobjects.Node{
		testCpuNode(testPriorities),
	}
	scheduler := &LegacyScheduler{
		SchedulingConfig: configuration.SchedulingConfig{},
		ExecutorId:       "executor",
		MinimumJobSize:   make(map[string]resource.Quantity),
	}

	jobsByQueue := map[string][]*api.Job{}
	for _, req := range testNSmallCpuJob(10) {
		jobsByQueue["A"] = append(
			jobsByQueue["A"],
			apiJobFromPodSpec("A", podSpecFromPodRequirements(req)),
		)
	}

	expectedScheduledJobs := make([]*api.Job, 0)
	for _, jobs := range jobsByQueue {
		expectedScheduledJobs = append(expectedScheduledJobs, jobs...)
	}
	expectedScheduledJobsByQueue := jobIdsByQueueFromJobs(expectedScheduledJobs)

	initialUsageByQueue := map[string]schedulerobjects.QuantityByPriorityAndResourceType{
		"A": {},
	}

	testSchedule(nodes, jobsByQueue, initialUsageByQueue, scheduler, expectedScheduledJobsByQueue, t)
}

func TestScheduleTwoQueues(t *testing.T) {
	nodes := []*schedulerobjects.Node{
		testCpuNode(testPriorities),
	}
	scheduler := &LegacyScheduler{
		SchedulingConfig: configuration.SchedulingConfig{},
		ExecutorId:       "executor",
		MinimumJobSize:   make(map[string]resource.Quantity),
	}

	jobsByQueue := map[string][]*api.Job{}
	for _, req := range testNSmallCpuJob(10) {
		jobsByQueue["A"] = append(
			jobsByQueue["A"],
			apiJobFromPodSpec("A", podSpecFromPodRequirements(req)),
		)
	}
	for _, req := range testNSmallCpuJob(10) {
		jobsByQueue["B"] = append(
			jobsByQueue["B"],
			apiJobFromPodSpec("B", podSpecFromPodRequirements(req)),
		)
	}

	expectedScheduledJobs := make([]*api.Job, 0)
	for _, jobs := range jobsByQueue {
		expectedScheduledJobs = append(expectedScheduledJobs, jobs...)
	}
	expectedScheduledJobsByQueue := jobIdsByQueueFromJobs(expectedScheduledJobs)

	initialUsageByQueue := map[string]schedulerobjects.QuantityByPriorityAndResourceType{
		"A": {},
		"B": {},
	}

	testSchedule(nodes, jobsByQueue, initialUsageByQueue, scheduler, expectedScheduledJobsByQueue, t)
}

func TestScheduleOne(t *testing.T) {

	nodes := []*schedulerobjects.Node{
		testCpuNode(testPriorities),
	}
	nodeDb, err := NewNodeDb(testPriorities, testResources)
	if !assert.NoError(t, err) {
		return
	}
	err = nodeDb.Upsert(nodes)
	if !assert.NoError(t, err) {
		return
	}

	jobQueue := newFakeJobQueue()
	legacyScheduler := &LegacyScheduler{
		SchedulingConfig: configuration.SchedulingConfig{},
		ExecutorId:       "executor",
		NodeDb:           nodeDb,
		JobQueue:         jobQueue,
		MinimumJobSize:   make(map[string]resource.Quantity),
	}

	queue := "queue"
	jobQueue.jobsByQueue[queue] = []*api.Job{
		apiJobFromPodSpec(queue, podSpecFromPodRequirements(testSmallCpuJob())),
	}

	initialUsageByQueue := map[string]schedulerobjects.QuantityByPriorityAndResourceType{
		queue: {
			0: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu": resource.MustParse("1"),
				},
			},
		},
	}

	expectedScheduledJobs := jobIdsByQueueFromJobs(jobQueue.jobsByQueue[queue])

	jobs, err := legacyScheduler.Schedule(context.Background(), initialUsageByQueue)
	if !assert.NoError(t, err) {
		return
	}

	actualScheduledJobs := jobIdsByQueueFromJobs(jobs)

	assert.Equal(t, expectedScheduledJobs, actualScheduledJobs)
	assert.Equal(t, 1, len(legacyScheduler.JobSchedulingReportsByQueue))
	assert.Equal(t, 1, len(legacyScheduler.JobSchedulingReportsByQueue[queue]))

	// for _, reports := range legacyScheduler.JobSchedulingReportsByQueue {
	// 	for _, jobReport := range reports {
	// 		fmt.Println(jobReport)
	// 	}
	// }
}

func jobIdsByQueueFromJobs(jobs []*api.Job) map[string][]string {
	rv := make(map[string][]string)
	for _, job := range jobs {
		rv[job.Queue] = append(rv[job.Queue], job.Id)
	}
	return rv
}

func apiJobFromPodSpec(queue string, podSpec *v1.PodSpec) *api.Job {
	return &api.Job{
		Id:      util.NewULID(),
		PodSpec: podSpec,
		Queue:   queue,
	}
}

func podSpecFromPodRequirements(req *schedulerobjects.PodRequirements) *v1.PodSpec {
	return &v1.PodSpec{
		NodeSelector:     req.NodeSelector,
		Affinity:         req.Affinity,
		Tolerations:      req.Tolerations,
		Priority:         &req.Priority,
		PreemptionPolicy: (*v1.PreemptionPolicy)(&req.PreemptionPolicy),
		Containers: []v1.Container{
			{
				Resources: req.ResourceRequirements,
			},
		},
	}
}

type fakeJobQueue struct {
	jobsByQueue map[string][]*api.Job
}

func newFakeJobQueue() *fakeJobQueue {
	return &fakeJobQueue{
		jobsByQueue: make(map[string][]*api.Job),
	}
}

func (r *fakeJobQueue) PeekClusterQueue(clusterId, queue string, limit int64) ([]*api.Job, error) {
	jobs := r.jobsByQueue[queue]
	fmt.Println("there are ", len(jobs), " jobs for queue ", queue)
	if len(jobs) == 0 {
		return make([]*api.Job, 0), nil
	}
	if int64(len(jobs)) > limit {
		return jobs[:limit], nil
	}
	return jobs, nil
}

func (r *fakeJobQueue) TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error) {
	remainingJobs := []*api.Job{}
outer:
	for _, j := range r.jobsByQueue[queue] {
		for _, l := range jobs {
			if j == l {
				continue outer
			}
		}
		remainingJobs = append(remainingJobs, j)
	}
	r.jobsByQueue[queue] = remainingJobs
	return jobs, nil
}
