package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestExtractQueueAndJobContexts(t *testing.T) {
	sctx := withUnsuccessfulJobSchedulingContext(withSuccessfulJobSchedulingContext(testSchedulingContext("executor"), "queue", "success"), "queue", "failure")
	queueSchedulingContextByQueue, jobSchedulingContextByJobId := extractQueueAndJobContexts(sctx)
	assert.Equal(
		t,
		withUnsuccessfulJobSchedulingContext(withSuccessfulJobSchedulingContext(testSchedulingContext("executor"), "queue", "success"), "queue", "failure"),
		sctx,
	)
	assert.Equal(
		t,
		withUnsuccessfulJobSchedulingContext(withSuccessfulJobSchedulingContext(testSchedulingContext("executor"), "queue", "success"), "queue", "failure").QueueSchedulingContexts,
		queueSchedulingContextByQueue,
	)
	assert.Equal(
		t,
		map[string]*schedulercontext.JobSchedulingContext{
			"success": {
				ExecutorId: "executor",
				JobId:      "success",
			},
			"failure": {
				ExecutorId:          "executor",
				JobId:               "failure",
				UnschedulableReason: "unknown",
			},
		},
		jobSchedulingContextByJobId,
	)
}

func TestAddGetSchedulingContext(t *testing.T) {
	repo, err := NewSchedulingContextRepository(10)
	require.NoError(t, err)

	sctx := testSchedulingContext("foo")
	sctx = withSuccessfulJobSchedulingContext(sctx, "A", "successFooA")
	err = repo.AddSchedulingContext(sctx)
	require.NoError(t, err)

	sctx = testSchedulingContext("foo")
	sctx = withUnsuccessfulJobSchedulingContext(sctx, "A", "failureA")
	err = repo.AddSchedulingContext(sctx)
	require.NoError(t, err)

	sctx = testSchedulingContext("bar")
	sctx = withUnsuccessfulJobSchedulingContext(sctx, "A", "failureA")
	sctx = withSuccessfulJobSchedulingContext(sctx, "B", "successBarB")
	err = repo.AddSchedulingContext(sctx)
	require.NoError(t, err)

	sctx = testSchedulingContext("bar")
	sctx = withUnsuccessfulJobSchedulingContext(sctx, "B", "failureB")
	err = repo.AddSchedulingContext(sctx)
	require.NoError(t, err)

	sctx = testSchedulingContext("baz")
	sctx = withPreemptingJobSchedulingContext(sctx, "C", "preempted")
	err = repo.AddSchedulingContext(sctx)
	require.NoError(t, err)

	actualJobSchedulingContextByExecutor, ok := repo.GetMostRecentJobSchedulingContextByExecutor("doesNotExist")
	require.Nil(t, actualJobSchedulingContextByExecutor)
	require.False(t, ok)

	actualJobSchedulingContextByExecutor, ok = repo.GetMostRecentJobSchedulingContextByExecutor("successFooA")
	require.True(t, ok)
	assert.Equal(
		t,
		JobSchedulingContextByExecutor{
			"foo": withSuccessfulJobSchedulingContext(testSchedulingContext("foo"), "A", "successFooA").QueueSchedulingContexts["A"].SuccessfulJobSchedulingContexts["successFooA"],
		},
		actualJobSchedulingContextByExecutor,
	)

	actualJobSchedulingContextByExecutor, ok = repo.GetMostRecentJobSchedulingContextByExecutor("failureA")
	require.True(t, ok)
	assert.Equal(
		t,
		JobSchedulingContextByExecutor{
			"foo": withUnsuccessfulJobSchedulingContext(testSchedulingContext("foo"), "A", "failureA").QueueSchedulingContexts["A"].UnsuccessfulJobSchedulingContexts["failureA"],
			"bar": withUnsuccessfulJobSchedulingContext(testSchedulingContext("bar"), "A", "failureA").QueueSchedulingContexts["A"].UnsuccessfulJobSchedulingContexts["failureA"],
		},
		actualJobSchedulingContextByExecutor,
	)

	actualQueueSchedulingContextByExecutor, ok := repo.GetMostRecentQueueSchedulingContextByExecutor("doesNotExist")
	require.Nil(t, actualQueueSchedulingContextByExecutor)
	require.False(t, ok)

	actualQueueSchedulingContextByExecutor, ok = repo.GetMostRecentQueueSchedulingContextByExecutor("A")
	require.True(t, ok)
	assert.Equal(
		t,
		QueueSchedulingContextByExecutor{
			"foo": withUnsuccessfulJobSchedulingContext(testSchedulingContext("foo"), "A", "failureA").QueueSchedulingContexts["A"],
			"bar": withUnsuccessfulJobSchedulingContext(testSchedulingContext("bar"), "A", "failureA").QueueSchedulingContexts["A"],
		},
		actualQueueSchedulingContextByExecutor,
	)

	actualQueueSchedulingContextByExecutor, ok = repo.GetMostRecentSuccessfulQueueSchedulingContextByExecutor("A")
	require.True(t, ok)
	assert.Equal(
		t,
		QueueSchedulingContextByExecutor{
			"foo": withSuccessfulJobSchedulingContext(testSchedulingContext("foo"), "A", "successFooA").QueueSchedulingContexts["A"],
		},
		actualQueueSchedulingContextByExecutor,
	)

	actualQueueSchedulingContextByExecutor, ok = repo.GetMostRecentQueueSchedulingContextByExecutor("B")
	require.True(t, ok)
	assert.Equal(
		t,
		QueueSchedulingContextByExecutor{
			"bar": withUnsuccessfulJobSchedulingContext(testSchedulingContext("bar"), "B", "failureB").QueueSchedulingContexts["B"],
		},
		actualQueueSchedulingContextByExecutor,
	)

	actualQueueSchedulingContextByExecutor, ok = repo.GetMostRecentSuccessfulQueueSchedulingContextByExecutor("B")
	require.True(t, ok)
	assert.Equal(
		t,
		QueueSchedulingContextByExecutor{
			"bar": withSuccessfulJobSchedulingContext(testSchedulingContext("bar"), "B", "successBarB").QueueSchedulingContexts["B"],
		},
		actualQueueSchedulingContextByExecutor,
	)

	actualQueueSchedulingContextByExecutor, ok = repo.GetMostRecentQueueSchedulingContextByExecutor("C")
	require.True(t, ok)
	assert.Equal(
		t,
		QueueSchedulingContextByExecutor{
			"baz": withPreemptingJobSchedulingContext(testSchedulingContext("baz"), "C", "preempted").QueueSchedulingContexts["C"],
		},
		actualQueueSchedulingContextByExecutor,
	)

	actualSchedulingContextByExecutor := repo.GetMostRecentSchedulingContextByExecutor()
	assert.Equal(
		t,
		SchedulingContextByExecutor{
			"foo": withUnsuccessfulJobSchedulingContext(testSchedulingContext("foo"), "A", "failureA"),
			"bar": withUnsuccessfulJobSchedulingContext(testSchedulingContext("bar"), "B", "failureB"),
			"baz": withPreemptingJobSchedulingContext(testSchedulingContext("baz"), "C", "preempted"),
		},
		actualSchedulingContextByExecutor,
	)

	actualSchedulingContextByExecutor = repo.GetMostRecentSuccessfulSchedulingContextByExecutor()
	assert.Equal(
		t,
		SchedulingContextByExecutor{
			"foo": withSuccessfulJobSchedulingContext(testSchedulingContext("foo"), "A", "successFooA"),
			"bar": withSuccessfulJobSchedulingContext(withUnsuccessfulJobSchedulingContext(testSchedulingContext("bar"), "A", "failureA"), "B", "successBarB"),
		},
		actualSchedulingContextByExecutor,
	)

	actualSchedulingContextByExecutor = repo.GetMostRecentPreemptingSchedulingContextByExecutor()
	assert.Equal(
		t,
		SchedulingContextByExecutor{
			"baz": withPreemptingJobSchedulingContext(testSchedulingContext("baz"), "C", "preempted"),
		},
		actualSchedulingContextByExecutor,
	)
}

// Concurrently write/read to/from the repo to test that there are no panics.
func TestTestAddGetSchedulingContextConcurrency(t *testing.T) {
	repo, err := NewSchedulingContextRepository(10)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, executorId := range []string{"foo", "bar"} {
		go func(executorId string) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				sctx := testSchedulingContext(executorId)
				sctx = withUnsuccessfulJobSchedulingContext(sctx, "A", "failureA")
				sctx = withUnsuccessfulJobSchedulingContext(sctx, "B", "failureB")
				sctx = withUnsuccessfulJobSchedulingContext(sctx, "C", "failureC")
				sctx = withSuccessfulJobSchedulingContext(sctx, "B", fmt.Sprintf("success%sB", executorId))
				sctx = withPreemptingJobSchedulingContext(sctx, "C", "preempted")
				err = repo.AddSchedulingContext(sctx)
				require.NoError(t, err)
				err = repo.AddSchedulingContext(sctx)
				require.NoError(t, err)
			}
		}(executorId)
	}
	for _, queue := range []string{"A", "B"} {
		go func(queue string) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			repo.getJobReportString(fmt.Sprintf("failure%s", queue))
			repo.getQueueReportString(queue, 0)
			repo.getSchedulingReport().ReportString(0)
		}(queue)
	}
	<-ctx.Done()
}

func withSuccessfulJobSchedulingContext(sctx *schedulercontext.SchedulingContext, queue, jobId string) *schedulercontext.SchedulingContext {
	if sctx.QueueSchedulingContexts == nil {
		sctx.QueueSchedulingContexts = make(map[string]*schedulercontext.QueueSchedulingContext)
	}
	qctx := sctx.QueueSchedulingContexts[queue]
	if qctx == nil {
		if err := sctx.AddQueueSchedulingContext(queue, 1.0, make(schedulerobjects.QuantityByPriorityAndResourceType)); err != nil {
			panic(err)
		}
		qctx = sctx.QueueSchedulingContexts[queue]
		qctx.SchedulingContext = nil
		qctx.Created = time.Time{}
	}
	qctx.SuccessfulJobSchedulingContexts[jobId] = &schedulercontext.JobSchedulingContext{
		ExecutorId: sctx.ExecutorId,
		JobId:      jobId,
	}
	rl := schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}}
	qctx.ScheduledResourcesByPriority.AddResourceList(0, rl)
	sctx.ScheduledResourcesByPriority.AddResourceList(0, rl)
	return sctx
}

func withPreemptingJobSchedulingContext(sctx *schedulercontext.SchedulingContext, queue, jobId string) *schedulercontext.SchedulingContext {
	if sctx.QueueSchedulingContexts == nil {
		sctx.QueueSchedulingContexts = make(map[string]*schedulercontext.QueueSchedulingContext)
	}
	qctx := sctx.QueueSchedulingContexts[queue]
	if qctx == nil {
		if err := sctx.AddQueueSchedulingContext(queue, 1.0, make(schedulerobjects.QuantityByPriorityAndResourceType)); err != nil {
			panic(err)
		}
		qctx = sctx.QueueSchedulingContexts[queue]
		qctx.SchedulingContext = nil
		qctx.Created = time.Time{}
	}
	qctx.EvictedJobsById[jobId] = true
	rl := schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}}
	qctx.EvictedResourcesByPriority.AddResourceList(0, rl)
	sctx.EvictedResourcesByPriority.AddResourceList(0, rl)
	return sctx
}

func withUnsuccessfulJobSchedulingContext(sctx *schedulercontext.SchedulingContext, queue, jobId string) *schedulercontext.SchedulingContext {
	if sctx.QueueSchedulingContexts == nil {
		sctx.QueueSchedulingContexts = make(map[string]*schedulercontext.QueueSchedulingContext)
	}
	qctx := sctx.QueueSchedulingContexts[queue]
	if qctx == nil {
		if err := sctx.AddQueueSchedulingContext(queue, 1.0, make(schedulerobjects.QuantityByPriorityAndResourceType)); err != nil {
			panic(err)
		}
		qctx = sctx.QueueSchedulingContexts[queue]
		qctx.SchedulingContext = nil
		qctx.Created = time.Time{}
	}
	qctx.UnsuccessfulJobSchedulingContexts[jobId] = &schedulercontext.JobSchedulingContext{
		ExecutorId:          sctx.ExecutorId,
		JobId:               jobId,
		UnschedulableReason: "unknown",
	}
	return sctx
}

func testSchedulingContext(executorId string) *schedulercontext.SchedulingContext {
	sctx := schedulercontext.NewSchedulingContext(
		executorId,
		"",
		nil,
		"",
		nil,
		schedulerobjects.ResourceList{},
	)
	sctx.Started = time.Time{}
	sctx.Finished = time.Time{}
	return sctx
}
