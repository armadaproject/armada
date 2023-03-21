package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormaliseSchedulingContext(t *testing.T) {
	sctx := testSchedulingContext("executor", "queue", "success", "failure")
	queueSchedulingContextByQueue, jobSchedulingContextByJobId := normaliseSchedulingContext(sctx)
	assert.Equal(
		t,
		map[string]*QueueSchedulingContext{
			"queue": nil,
		},
		sctx.QueueSchedulingContexts,
	)
	assert.Equal(
		t,
		map[string]*QueueSchedulingContext{
			"queue": {
				ExecutorId: "executor",
				SuccessfulJobSchedulingContexts: map[string]*JobSchedulingContext{
					"success": nil,
				},
				UnsuccessfulJobSchedulingContexts: map[string]*JobSchedulingContext{
					"failure": nil,
				},
			},
		},
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
	// Create a context.
	// Create some queue contexts.
	// Create some job contexts.
	// Insert
	// Get
	// Make sure I get the same thing back as I put in.

	// sctx := NewSchedulingContext(
	// 	"executor",
	// 	schedulerobjects.ResourceList{
	// 		Resources: map[string]resource.Quantity{
	// 			"cpu": resource.MustParse("1"),
	// 			"memory": resource.MustParse("10Mi"),
	// 		},
	// 	},
	// 	nil,
	// 	nil,
	// )
	// sctxFoo := &SchedulingContext{
	// 	ExecutorId: "foo",
	// 	QueueSchedulingContexts: map[string]*QueueSchedulingContext{
	// 		"A": {
	// 			ExecutorId: "foo",
	// 			SuccessfulJobSchedulingContexts: map[string]*JobSchedulingContext{
	// 				"myJobIdFoo": {
	// 					ExecutorId: "foo",
	// 					JobId:      "myJobIdFoo",
	// 				},
	// 			},
	// 		},
	// 	},
	// }
	// sctxBar := &SchedulingContext{
	// 	ExecutorId: "bar",
	// 	QueueSchedulingContexts: map[string]*QueueSchedulingContext{
	// 		"B": {
	// 			ExecutorId: "bar",
	// 			SuccessfulJobSchedulingContexts: map[string]*JobSchedulingContext{
	// 				"myJobIdBar": {
	// 					ExecutorId: "bar",
	// 					JobId:      "myJobIdBar",
	// 				},
	// 			},
	// 		},
	// 	},
	// }

	repo, err := NewSchedulingContextRepository(10)
	require.NoError(t, err)
	repo.AddSchedulingContext(testSchedulingContext("foo", "A", "successA", "failureA"))
	repo.AddSchedulingContext(testSchedulingContext("bar", "A", "successA", "failureA"))
	repo.AddSchedulingContext(testSchedulingContext("bar", "B", "successB", "failureB"))

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

	// schedulingContextByExecutor := repo.GetSchedulingContextByExecutor()

	// fmt.Println(schedulingContextByExecutor["foo"].QueueSchedulingContexts["A"])
	// assert.Equal(
	// 	t,
	// 	SchedulingContextByExecutor{
	// 		"foo": testSchedulingContext("foo", "A", "successFooA", "failureFooA"),
	// 	},
	// 	schedulingContextByExecutor,
	// )
}

func testSchedulingContext(executorId, queue, successJobId, unsuccessfulJobId string) *SchedulingContext {
	return &SchedulingContext{
		ExecutorId: executorId,
		QueueSchedulingContexts: map[string]*QueueSchedulingContext{
			queue: {
				ExecutorId: executorId,
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
