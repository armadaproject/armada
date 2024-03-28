package reports

import (
	"testing"

	"github.com/stretchr/testify/assert"

	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
)

var jobContextA = &schedulercontext.JobSchedulingContext{
	JobId: "testJob1",
}

var jobContextB = &schedulercontext.JobSchedulingContext{
	JobId: "testJob2",
}

var testContextA = &schedulercontext.SchedulingContext{
	Pool: "poolA",
	QueueSchedulingContexts: map[string]*schedulercontext.QueueSchedulingContext{
		"queueA": {
			SuccessfulJobSchedulingContexts: map[string]*schedulercontext.JobSchedulingContext{
				"testJob1": jobContextA,
			},
			UnsuccessfulJobSchedulingContexts: map[string]*schedulercontext.JobSchedulingContext{
				"testJob2": jobContextA,
			},
		},
	},
}

var testContextB = &schedulercontext.SchedulingContext{
	Pool: "poolB",
	QueueSchedulingContexts: map[string]*schedulercontext.QueueSchedulingContext{
		"queueA": {
			SuccessfulJobSchedulingContexts: map[string]*schedulercontext.JobSchedulingContext{},
			UnsuccessfulJobSchedulingContexts: map[string]*schedulercontext.JobSchedulingContext{
				"testJob1": jobContextA,
				"testJob2": jobContextB,
			},
		},
	},
}

func TestJobSchedulingContext(t *testing.T) {
	tests := map[string]struct {
		inputContexts    []*schedulercontext.SchedulingContext
		jobId            string
		expectedContexts []CtxPoolPair[*schedulercontext.JobSchedulingContext]
	}{
		"No contexts": {
			inputContexts:    []*schedulercontext.SchedulingContext{},
			jobId:            "testJob1",
			expectedContexts: []CtxPoolPair[*schedulercontext.JobSchedulingContext]{},
		},
		"Job present in all pools": {
			inputContexts: []*schedulercontext.SchedulingContext{testContextA, testContextB},
			jobId:         "testJob1",
			expectedContexts: []CtxPoolPair[*schedulercontext.JobSchedulingContext]{
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
			inputContexts: []*schedulercontext.SchedulingContext{testContextA, testContextB},
			jobId:         "testJob2",
			expectedContexts: []CtxPoolPair[*schedulercontext.JobSchedulingContext]{
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
			inputContexts: []*schedulercontext.SchedulingContext{testContextA, testContextB},
			jobId:         "testJob3",
			expectedContexts: []CtxPoolPair[*schedulercontext.JobSchedulingContext]{
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
		inputContexts    []*schedulercontext.SchedulingContext
		queueId          string
		expectedContexts []CtxPoolPair[*schedulercontext.QueueSchedulingContext]
	}{
		"No contexts": {
			inputContexts:    []*schedulercontext.SchedulingContext{},
			queueId:          "testJob1",
			expectedContexts: []CtxPoolPair[*schedulercontext.QueueSchedulingContext]{},
		},
		"Queue present in all pools": {
			inputContexts: []*schedulercontext.SchedulingContext{testContextA, testContextB},
			queueId:       "queueA",
			expectedContexts: []CtxPoolPair[*schedulercontext.QueueSchedulingContext]{
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
			inputContexts: []*schedulercontext.SchedulingContext{testContextA, testContextB},
			queueId:       "queueC",
			expectedContexts: []CtxPoolPair[*schedulercontext.QueueSchedulingContext]{
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
		inputContexts    []*schedulercontext.SchedulingContext
		expectedContexts []CtxPoolPair[*schedulercontext.SchedulingContext]
	}{
		"No contexts": {
			inputContexts:    []*schedulercontext.SchedulingContext{},
			expectedContexts: []CtxPoolPair[*schedulercontext.SchedulingContext]{},
		},
		"Contexts": {
			inputContexts: []*schedulercontext.SchedulingContext{testContextA, testContextB},
			expectedContexts: []CtxPoolPair[*schedulercontext.SchedulingContext]{
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
