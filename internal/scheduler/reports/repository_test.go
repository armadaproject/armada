package reports

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

var jobContextA = &context.JobSchedulingContext{
	JobId: "testJob1",
}

var jobContextB = &context.JobSchedulingContext{
	JobId: "testJob2",
}

var testContextA = &context.SchedulingContext{
	Pool: "poolA",
	QueueSchedulingContexts: map[string]*context.QueueSchedulingContext{
		"queueA": {
			SuccessfulJobSchedulingContexts: map[string]*context.JobSchedulingContext{
				"testJob1": jobContextA,
			},
			UnsuccessfulJobSchedulingContexts: map[string]*context.JobSchedulingContext{
				"testJob2": jobContextA,
			},
		},
	},
}

var testContextB = &context.SchedulingContext{
	Pool: "poolB",
	QueueSchedulingContexts: map[string]*context.QueueSchedulingContext{
		"queueA": {
			SuccessfulJobSchedulingContexts: map[string]*context.JobSchedulingContext{},
			UnsuccessfulJobSchedulingContexts: map[string]*context.JobSchedulingContext{
				"testJob1": jobContextA,
				"testJob2": jobContextB,
			},
		},
	},
}

func TestJobSchedulingContext(t *testing.T) {
	tests := map[string]struct {
		inputContexts    []*context.SchedulingContext
		jobId            string
		expectedContexts []CtxPoolPair[*context.JobSchedulingContext]
	}{
		"No contexts": {
			inputContexts:    []*context.SchedulingContext{},
			jobId:            "testJob1",
			expectedContexts: []CtxPoolPair[*context.JobSchedulingContext]{},
		},
		"Job present in all pools": {
			inputContexts: []*context.SchedulingContext{testContextA, testContextB},
			jobId:         "testJob1",
			expectedContexts: []CtxPoolPair[*context.JobSchedulingContext]{
				{
					pool:          "poolA",
					schedulingCtx: jobContextA,
				},
				{
					pool:          "poolB",
					schedulingCtx: jobContextA,
				},
			},
		},
		"Job present in one pool": {
			inputContexts: []*context.SchedulingContext{testContextA, testContextB},
			jobId:         "testJob2",
			expectedContexts: []CtxPoolPair[*context.JobSchedulingContext]{
				{
					pool: "poolA",
				},
				{
					pool:          "poolB",
					schedulingCtx: jobContextB,
				},
			},
		},
		"Job missing": {
			inputContexts: []*context.SchedulingContext{testContextA, testContextB},
			jobId:         "testJob3",
			expectedContexts: []CtxPoolPair[*context.JobSchedulingContext]{
				{
					pool: "poolA",
				},
				{
					pool: "poolB",
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			repo := NewSchedulingContextRepository()
			for _, ctx := range tc.inputContexts {
				repo.StoreSchedulingContext(ctx)
			}
			assert.Equal(t, tc.expectedContexts, repo.JobSchedulingContext(tc.jobId))
		})
	}
}

func TestQueueSchedulingContext(t *testing.T) {
	tests := map[string]struct {
		inputContexts    []*context.SchedulingContext
		queueId          string
		expectedContexts []CtxPoolPair[*context.QueueSchedulingContext]
	}{
		"No contexts": {
			inputContexts:    []*context.SchedulingContext{},
			queueId:          "testJob1",
			expectedContexts: []CtxPoolPair[*context.QueueSchedulingContext]{},
		},
		"Queue present in all pools": {
			inputContexts: []*context.SchedulingContext{testContextA, testContextB},
			queueId:       "queueA",
			expectedContexts: []CtxPoolPair[*context.QueueSchedulingContext]{
				{
					pool:          "poolA",
					schedulingCtx: testContextA.QueueSchedulingContexts["queueA"],
				},
				{
					pool:          "poolB",
					schedulingCtx: testContextB.QueueSchedulingContexts["queueA"],
				},
			},
		},
		"Queue missing": {
			inputContexts: []*context.SchedulingContext{testContextA, testContextB},
			queueId:       "queueC",
			expectedContexts: []CtxPoolPair[*context.QueueSchedulingContext]{
				{
					pool: "poolA",
				},
				{
					pool: "poolB",
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			repo := NewSchedulingContextRepository()
			for _, ctx := range tc.inputContexts {
				repo.StoreSchedulingContext(ctx)
			}
			assert.Equal(t, tc.expectedContexts, repo.QueueSchedulingContext(tc.queueId))
		})
	}
}

func TestRoundSchedulingContext(t *testing.T) {
	tests := map[string]struct {
		inputContexts    []*context.SchedulingContext
		expectedContexts []CtxPoolPair[*context.SchedulingContext]
	}{
		"No contexts": {
			inputContexts:    []*context.SchedulingContext{},
			expectedContexts: []CtxPoolPair[*context.SchedulingContext]{},
		},
		"Contexts": {
			inputContexts: []*context.SchedulingContext{testContextA, testContextB},
			expectedContexts: []CtxPoolPair[*context.SchedulingContext]{
				{
					pool:          "poolA",
					schedulingCtx: testContextA,
				},
				{
					pool:          "poolB",
					schedulingCtx: testContextB,
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			repo := NewSchedulingContextRepository()
			for _, ctx := range tc.inputContexts {
				repo.StoreSchedulingContext(ctx)
			}
			assert.Equal(t, tc.expectedContexts, repo.RoundSchedulingContext())
		})
	}
}
