package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractQueueAndJobContexts(t *testing.T) {
	sctx := testSchedulingContext("executor", "queue", "success", "failure")
	queueSchedulingContextByQueue, jobSchedulingContextByJobId := extractQueueAndJobContexts(sctx)
	assert.Equal(
		t,
		testSchedulingContext("executor", "queue", "success", "failure"),
		sctx,
	)
	assert.Equal(
		t,
		testSchedulingContext("executor", "queue", "success", "failure").QueueSchedulingContexts,
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
				ExecutorId: "executor",
				JobId:      "failure",
			},
		},
		jobSchedulingContextByJobId,
	)
}

func TestAddGetSchedulingContext(t *testing.T) {
	repo, err := NewSchedulingContextRepository(10)
	require.NoError(t, err)
	err = repo.AddSchedulingContext(testSchedulingContext("foo", "A", "successA", "failureA"))
	require.NoError(t, err)
	err = repo.AddSchedulingContext(testSchedulingContext("bar", "A", "successA", "failureA"))
	require.NoError(t, err)
	err = repo.AddSchedulingContext(testSchedulingContext("bar", "B", "successB", "failureB"))
	require.NoError(t, err)

	actualJobSchedulingContextByExecutor, ok := repo.GetJobSchedulingContextByExecutor("doesNotExist")
	require.Nil(t, actualJobSchedulingContextByExecutor)
	require.False(t, ok)

	actualJobSchedulingContextByExecutor, ok = repo.GetJobSchedulingContextByExecutor("successA")
	require.True(t, ok)
	assert.Equal(
		t,
		JobSchedulingContextByExecutor{
			"foo": testSchedulingContext("foo", "A", "successA", "failureA").QueueSchedulingContexts["A"].SuccessfulJobSchedulingContexts["successA"],
			"bar": testSchedulingContext("bar", "A", "successA", "failureA").QueueSchedulingContexts["A"].SuccessfulJobSchedulingContexts["successA"],
		},
		actualJobSchedulingContextByExecutor,
	)

	actualJobSchedulingContextByExecutor, ok = repo.GetJobSchedulingContextByExecutor("failureB")
	require.True(t, ok)
	assert.Equal(
		t,
		JobSchedulingContextByExecutor{
			"bar": testSchedulingContext("bar", "B", "successB", "failureB").QueueSchedulingContexts["B"].UnsuccessfulJobSchedulingContexts["failureB"],
		},
		actualJobSchedulingContextByExecutor,
	)

	actualQueueSchedulingContextByExecutor, ok := repo.GetQueueSchedulingContextByExecutor("doesNotExist")
	require.Nil(t, actualQueueSchedulingContextByExecutor)
	require.False(t, ok)

	actualQueueSchedulingContextByExecutor, ok = repo.GetQueueSchedulingContextByExecutor("A")
	require.True(t, ok)
	assert.Equal(
		t,
		QueueSchedulingContextByExecutor{
			"foo": testSchedulingContext("foo", "A", "successA", "failureA").QueueSchedulingContexts["A"],
			"bar": testSchedulingContext("bar", "A", "successA", "failureA").QueueSchedulingContexts["A"],
		},
		actualQueueSchedulingContextByExecutor,
	)

	actualSchedulingContextByExecutor := repo.GetSchedulingContextByExecutor()
	assert.Equal(
		t,
		SchedulingContextByExecutor{
			"foo": testSchedulingContext("foo", "A", "successA", "failureA"),
			"bar": testSchedulingContext("bar", "B", "successB", "failureB"),
		},
		actualSchedulingContextByExecutor,
	)
}

func testSchedulingContext(executorId, queue, successJobId, unsuccessfulJobId string) *SchedulingContext {
	return &SchedulingContext{
		ExecutorId: executorId,
		QueueSchedulingContexts: map[string]*QueueSchedulingContext{
			queue: {
				ExecutorId: executorId,
				Queue:      queue,
				SuccessfulJobSchedulingContexts: map[string]*JobSchedulingContext{
					successJobId: {
						ExecutorId: executorId,
						JobId:      successJobId,
					},
				},
				UnsuccessfulJobSchedulingContexts: map[string]*JobSchedulingContext{
					unsuccessfulJobId: {
						ExecutorId: executorId,
						JobId:      unsuccessfulJobId,
					},
				},
			},
		},
	}
}
