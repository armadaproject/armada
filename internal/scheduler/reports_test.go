package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

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
		map[string]*JobSchedulingContext{
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

	actualSchedulingContextByExecutor := repo.GetMostRecentSchedulingContextByExecutor()
	assert.Equal(
		t,
		SchedulingContextByExecutor{
			"foo": withUnsuccessfulJobSchedulingContext(testSchedulingContext("foo"), "A", "failureA"),
			"bar": withUnsuccessfulJobSchedulingContext(testSchedulingContext("bar"), "B", "failureB"),
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
				sctx = withSuccessfulJobSchedulingContext(sctx, "B", fmt.Sprintf("success%sB", executorId))
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
			repo.getQueueReportString(queue)
			repo.getSchedulingReportString()
		}(queue)
	}
	<-ctx.Done()
}

func withSuccessfulJobSchedulingContext(sctx *SchedulingContext, queue, jobId string) *SchedulingContext {
	if sctx.QueueSchedulingContexts == nil {
		sctx.QueueSchedulingContexts = make(map[string]*QueueSchedulingContext)
	}
	qctx := sctx.QueueSchedulingContexts[queue]
	if qctx == nil {
		qctx = NewQueueSchedulingContext(queue, sctx.ExecutorId, 1.0, make(schedulerobjects.QuantityByPriorityAndResourceType))
		qctx.Created = time.Time{}
		sctx.QueueSchedulingContexts[queue] = qctx
	}
	qctx.SuccessfulJobSchedulingContexts[jobId] = &JobSchedulingContext{
		ExecutorId: sctx.ExecutorId,
		JobId:      jobId,
	}
	qctx.ScheduledResourcesByPriority.AddResourceList(
		0,
		schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")},
		},
	)
	sctx.ScheduledResourcesByPriority.AddResourceList(
		0,
		schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")},
		},
	)
	return sctx
}

func withUnsuccessfulJobSchedulingContext(sctx *SchedulingContext, queue, jobId string) *SchedulingContext {
	if sctx.QueueSchedulingContexts == nil {
		sctx.QueueSchedulingContexts = make(map[string]*QueueSchedulingContext)
	}
	qctx := sctx.QueueSchedulingContexts[queue]
	if qctx == nil {
		qctx = NewQueueSchedulingContext(queue, sctx.ExecutorId, 1.0, make(schedulerobjects.QuantityByPriorityAndResourceType))
		qctx.Created = time.Time{}
		sctx.QueueSchedulingContexts[queue] = qctx
	}
	qctx.UnsuccessfulJobSchedulingContexts[jobId] = &JobSchedulingContext{
		ExecutorId:          sctx.ExecutorId,
		JobId:               jobId,
		UnschedulableReason: "unknown",
	}
	return sctx
}

func testSchedulingContext(executorId string) *SchedulingContext {
	sctx := NewSchedulingContext(executorId, schedulerobjects.ResourceList{}, nil, nil)
	sctx.Started = time.Time{}
	sctx.Finished = time.Time{}
	return sctx
}
