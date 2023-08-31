package scheduler

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common/context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/util"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

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

	var actualSchedulingContextByExecutor SchedulingContextByExecutor
	var ok bool

	actualSchedulingContextByExecutor, ok = repo.GetMostRecentSchedulingContextByExecutorForJob("doesNotExist")
	require.Nil(t, actualSchedulingContextByExecutor)
	require.False(t, ok)

	actualSchedulingContextByExecutor, ok = repo.GetMostRecentSchedulingContextByExecutorForJob("successFooA")
	require.True(t, ok)
	assert.Equal(
		t,
		SchedulingContextByExecutor{
			"foo": withSuccessfulJobSchedulingContext(testSchedulingContext("foo"), "A", "successFooA"),
		},
		actualSchedulingContextByExecutor,
	)

	actualSchedulingContextByExecutor, ok = repo.GetMostRecentSchedulingContextByExecutorForJob("failureA")
	require.True(t, ok)
	assert.Equal(
		t,
		withUnsuccessfulJobSchedulingContext(testSchedulingContext("foo"), "A", "failureA").QueueSchedulingContexts["A"],
		actualSchedulingContextByExecutor["foo"].QueueSchedulingContexts["A"],
	)
	assert.Equal(
		t,
		withUnsuccessfulJobSchedulingContext(testSchedulingContext("bar"), "A", "failureA").QueueSchedulingContexts["A"],
		actualSchedulingContextByExecutor["bar"].QueueSchedulingContexts["A"],
	)

	actualSchedulingContextByExecutor, ok = repo.GetMostRecentSchedulingContextByExecutorForQueue("doesNotExist")
	require.Nil(t, actualSchedulingContextByExecutor)
	require.False(t, ok)

	actualSchedulingContextByExecutor, ok = repo.GetMostRecentSchedulingContextByExecutorForQueue("A")
	require.True(t, ok)
	assert.Equal(
		t,
		withUnsuccessfulJobSchedulingContext(testSchedulingContext("foo"), "A", "failureA").QueueSchedulingContexts["A"],
		actualSchedulingContextByExecutor["foo"].QueueSchedulingContexts["A"],
	)
	assert.Equal(
		t,
		withUnsuccessfulJobSchedulingContext(testSchedulingContext("bar"), "A", "failureA").QueueSchedulingContexts["A"],
		actualSchedulingContextByExecutor["bar"].QueueSchedulingContexts["A"],
	)

	actualSchedulingContextByExecutor, ok = repo.GetMostRecentSuccessfulSchedulingContextByExecutorForQueue("A")
	require.True(t, ok)
	assert.Equal(
		t,
		withSuccessfulJobSchedulingContext(testSchedulingContext("foo"), "A", "successFooA").QueueSchedulingContexts["A"],
		actualSchedulingContextByExecutor["foo"].QueueSchedulingContexts["A"],
	)

	actualSchedulingContextByExecutor, ok = repo.GetMostRecentSchedulingContextByExecutorForQueue("B")
	require.True(t, ok)
	assert.Equal(
		t,
		withUnsuccessfulJobSchedulingContext(testSchedulingContext("bar"), "B", "failureB").QueueSchedulingContexts["B"],
		actualSchedulingContextByExecutor["bar"].QueueSchedulingContexts["B"],
	)

	actualSchedulingContextByExecutor, ok = repo.GetMostRecentSuccessfulSchedulingContextByExecutorForQueue("B")
	require.True(t, ok)
	assert.Equal(
		t,
		withSuccessfulJobSchedulingContext(testSchedulingContext("bar"), "B", "successBarB").QueueSchedulingContexts["B"],
		actualSchedulingContextByExecutor["bar"].QueueSchedulingContexts["B"],
	)

	actualSchedulingContextByExecutor, ok = repo.GetMostRecentSchedulingContextByExecutorForQueue("C")
	require.True(t, ok)
	assert.Equal(
		t,
		withPreemptingJobSchedulingContext(testSchedulingContext("baz"), "C", "preempted").QueueSchedulingContexts["C"],
		actualSchedulingContextByExecutor["baz"].QueueSchedulingContexts["C"],
	)

	actualSchedulingContextByExecutor = repo.GetMostRecentSchedulingContextByExecutor()
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
			repo.getSchedulingReportString(0)
		}(queue)
	}
	<-ctx.Done()
}

func TestReportDoesNotExist(t *testing.T) {
	repo, err := NewSchedulingContextRepository(1024)
	require.NoError(t, err)
	err = repo.AddSchedulingContext(testSchedulingContext("executor-01"))
	require.NoError(t, err)
	ctx := context.Background()
	queue := "queue-does-not-exist"
	jobId := util.NewULID()

	_, err = repo.GetSchedulingReport(ctx, &schedulerobjects.SchedulingReportRequest{})
	require.NoError(t, err)

	_, err = repo.GetSchedulingReport(
		ctx,
		&schedulerobjects.SchedulingReportRequest{
			Filter: &schedulerobjects.SchedulingReportRequest_MostRecentForQueue{
				MostRecentForQueue: &schedulerobjects.MostRecentForQueue{
					QueueName: queue,
				},
			},
		},
	)
	require.NoError(t, err)

	_, err = repo.GetSchedulingReport(
		ctx,
		&schedulerobjects.SchedulingReportRequest{
			Filter: &schedulerobjects.SchedulingReportRequest_MostRecentForJob{
				MostRecentForJob: &schedulerobjects.MostRecentForJob{
					JobId: jobId,
				},
			},
		},
	)
	require.NoError(t, err)

	_, err = repo.GetQueueReport(ctx, &schedulerobjects.QueueReportRequest{QueueName: queue})
	require.NoError(t, err)

	_, err = repo.GetJobReport(ctx, &schedulerobjects.JobReportRequest{JobId: jobId})
	require.NoError(t, err)
}

func withSuccessfulJobSchedulingContext(sctx *schedulercontext.SchedulingContext, queue, jobId string) *schedulercontext.SchedulingContext {
	if sctx.QueueSchedulingContexts == nil {
		sctx.QueueSchedulingContexts = make(map[string]*schedulercontext.QueueSchedulingContext)
	}
	qctx := sctx.QueueSchedulingContexts[queue]
	if qctx == nil {
		if err := sctx.AddQueueSchedulingContext(queue, 1.0, make(schedulerobjects.QuantityByTAndResourceType[string])); err != nil {
			panic(err)
		}
		qctx = sctx.QueueSchedulingContexts[queue]
		qctx.SchedulingContext = nil
		qctx.Created = time.Time{}
	}
	qctx.SuccessfulJobSchedulingContexts[jobId] = &schedulercontext.JobSchedulingContext{JobId: jobId}
	rl := schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}}
	qctx.ScheduledResourcesByPriorityClass.AddResourceList("foo", rl)
	sctx.ScheduledResourcesByPriorityClass.AddResourceList("foo", rl)
	return sctx
}

func withPreemptingJobSchedulingContext(sctx *schedulercontext.SchedulingContext, queue, jobId string) *schedulercontext.SchedulingContext {
	if sctx.QueueSchedulingContexts == nil {
		sctx.QueueSchedulingContexts = make(map[string]*schedulercontext.QueueSchedulingContext)
	}
	qctx := sctx.QueueSchedulingContexts[queue]
	if qctx == nil {
		if err := sctx.AddQueueSchedulingContext(queue, 1.0, make(schedulerobjects.QuantityByTAndResourceType[string])); err != nil {
			panic(err)
		}
		qctx = sctx.QueueSchedulingContexts[queue]
		qctx.SchedulingContext = nil
		qctx.Created = time.Time{}
	}
	qctx.EvictedJobsById[jobId] = true
	rl := schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}}
	qctx.EvictedResourcesByPriorityClass.AddResourceList("foo", rl)
	sctx.EvictedResourcesByPriorityClass.AddResourceList("foo", rl)
	return sctx
}

func withUnsuccessfulJobSchedulingContext(sctx *schedulercontext.SchedulingContext, queue, jobId string) *schedulercontext.SchedulingContext {
	if sctx.QueueSchedulingContexts == nil {
		sctx.QueueSchedulingContexts = make(map[string]*schedulercontext.QueueSchedulingContext)
	}
	qctx := sctx.QueueSchedulingContexts[queue]
	if qctx == nil {
		if err := sctx.AddQueueSchedulingContext(queue, 1.0, make(schedulerobjects.QuantityByTAndResourceType[string])); err != nil {
			panic(err)
		}
		qctx = sctx.QueueSchedulingContexts[queue]
		qctx.SchedulingContext = nil
		qctx.Created = time.Time{}
	}
	qctx.UnsuccessfulJobSchedulingContexts[jobId] = &schedulercontext.JobSchedulingContext{JobId: jobId, UnschedulableReason: "unknown"}
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
	sctx.SchedulingKeyGenerator = nil
	return sctx
}
