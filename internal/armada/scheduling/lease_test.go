package scheduling

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common"
)

func Test_matchRequirements(t *testing.T) {

	job := &api.Job{RequiredNodeLabels: map[string]string{"armada/region": "eu", "armada/zone": "1"}}

	assert.False(t, matchRequirements(job, &api.LeaseRequest{}))
	assert.False(t, matchRequirements(job, &api.LeaseRequest{AvailableLabels: []*api.NodeLabeling{
		{Labels: map[string]string{"armada/region": "eu"}},
		{Labels: map[string]string{"armada/zone": "2"}},
	}}))
	assert.False(t, matchRequirements(job, &api.LeaseRequest{AvailableLabels: []*api.NodeLabeling{
		{Labels: map[string]string{"armada/region": "eu", "armada/zone": "2"}},
	}}))

	assert.True(t, matchRequirements(job, &api.LeaseRequest{AvailableLabels: []*api.NodeLabeling{
		{Labels: map[string]string{"x": "y"}},
		{Labels: map[string]string{"armada/region": "eu", "armada/zone": "1", "x": "y"}},
	}}))
}

func Test_distributeRemainder_highPriorityUserDoesNotBlockOthers(t *testing.T) {

	queue1 := &api.Queue{Name: "queue1", PriorityFactor: 1}
	queue2 := &api.Queue{Name: "queue2", PriorityFactor: 1}

	scarcity := map[string]float64{"cpu": 1, "gpu": 1}

	priorities := map[*api.Queue]QueuePriorityInfo{
		queue1: {
			Priority:     1000,
			CurrentUsage: common.ComputeResources{"cpu": resource.MustParse("100"), "memory": resource.MustParse("80Gi")}},
		queue2: {
			Priority:     0.5,
			CurrentUsage: common.ComputeResources{"cpu": resource.MustParse("0"), "memory": resource.MustParse("0")}},
	}
	requestSize := common.ComputeResources{"cpu": resource.MustParse("10"), "memory": resource.MustParse("1Gi")}

	repository := &fakeJobQueueRepository{
		jobsByQueue: map[string][]*api.Job{
			"queue1": {
				&api.Job{PodSpec: classicPodSpec},
				&api.Job{PodSpec: classicPodSpec},
				&api.Job{PodSpec: classicPodSpec},
				&api.Job{PodSpec: classicPodSpec},
				&api.Job{PodSpec: classicPodSpec},
			},
			"queue2": {
				&api.Job{RequiredNodeLabels: map[string]string{"impossible": "label"}, PodSpec: classicPodSpec},
			},
		},
	}

	// the leasing logic stops scheduling 1s before the deadline
	ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))

	c := leaseContext{
		ctx: ctx,
		schedulingConfig: &configuration.SchedulingConfig{
			QueueLeaseBatchSize: 10,
		},
		onJobsLeased:     func(a []*api.Job) {},
		request:          &api.LeaseRequest{ClusterId: "c1", Resources: requestSize},
		resourceScarcity: scarcity,
		priorities:       priorities,
		slices:           SliceResource(scarcity, priorities, requestSize.AsFloat()),
		repository:       repository,
		queueCache:       map[string][]*api.Job{},
	}

	jobs, e := c.distributeRemainder(1000)
	assert.Nil(t, e)
	assert.Equal(t, 5, len(jobs))
}

var classicPodSpec = &v1.PodSpec{
	Containers: []v1.Container{{
		Name:  "Container1",
		Image: "index.docker.io/library/ubuntu:latest",
		Args:  []string{"sleep", "10s"},
		Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Mi")},
			Limits:   v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Mi")},
		}}}}

type fakeJobQueueRepository struct {
	jobsByQueue map[string][]*api.Job
}

func (r *fakeJobQueueRepository) PeekQueue(queue string, limit int64) ([]*api.Job, error) {
	jobs, exists := r.jobsByQueue[queue]
	if !exists {
		return []*api.Job{}, nil
	}
	if int64(len(jobs)) > limit {
		jobs = jobs[:limit]
	}
	return jobs, nil
}

func (r *fakeJobQueueRepository) TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error) {
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
