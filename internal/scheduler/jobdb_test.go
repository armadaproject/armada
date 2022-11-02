package scheduler

import (
	"math"
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
)

func TestJobDbSchema(t *testing.T) {
	err := jobDbSchema().Validate()
	assert.NoError(t, err)
}

func TestJobQueuePriorityClassIterator(t *testing.T) {
	tests := map[string]struct {
		Queue                 string
		MinPriorityClassValue uint32
		MinPriority           uint32
		Items                 []*SchedulerJob
		ExpectedOrder         []int
	}{
		"Queue A": {
			Queue:                 "A",
			MinPriorityClassValue: 0,
			MinPriority:           0,
			Items:                 testJobItems1(),
			ExpectedOrder:         intRange(0, 5),
		},
		"Queue B": {
			Queue:                 "B",
			MinPriorityClassValue: 0,
			MinPriority:           0,
			Items:                 testJobItems1(),
			ExpectedOrder:         intRange(6, 12),
		},
		"Queue C": {
			Queue:                 "C",
			MinPriorityClassValue: 0,
			MinPriority:           0,
			Items:                 testJobItems1(),
			ExpectedOrder:         intRange(13, 13),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			db, err := NewJobDb()
			if !assert.NoError(t, err) {
				return
			}

			// Shuffle and insert jobs.
			items := slices.Clone(tc.Items)
			slices.SortFunc(items, func(a, b *SchedulerJob) bool { return rand.Float64() < 0.5 })
			err = db.Upsert(items)
			if !assert.NoError(t, err) {
				return
			}

			// Test that jobs are returned in the expected order.
			txn := db.Db.Txn(false)
			it, err := NewJobQueueIterator(txn, tc.Queue)
			if !assert.NoError(t, err) {
				return
			}
			for _, i := range tc.ExpectedOrder {
				item := it.NextJobItem()
				if !assert.Equal(t, tc.Items[i], item) {
					return
				}
			}
			item := it.NextJobItem()
			if !assert.Nil(t, item) {
				return
			}
		})
	}
}

func testJobItems1() []*SchedulerJob {
	return []*SchedulerJob{
		{
			JobId:                     uuid.NewString(),
			Queue:                     "A",
			NegatedPriorityClassValue: -1,
			Priority:                  0,
			Timestamp:                 10,
			node:                      nil,
			jobSchedulingInfo:         nil,
		},
		{
			JobId:                     uuid.NewString(),
			Queue:                     "A",
			NegatedPriorityClassValue: -1,
			Priority:                  1,
			Timestamp:                 0,
		},
		{
			JobId:                     uuid.NewString(),
			Queue:                     "A",
			NegatedPriorityClassValue: -1,
			Priority:                  1,
			Timestamp:                 1,
		},
		{
			JobId:                     uuid.NewString(),
			Queue:                     "A",
			NegatedPriorityClassValue: 0,
			Priority:                  0,
		},
		{
			JobId:                     uuid.NewString(),
			Queue:                     "A",
			NegatedPriorityClassValue: 0,
			Priority:                  1,
		},
		{
			JobId:                     uuid.NewString(),
			Queue:                     "A",
			NegatedPriorityClassValue: 0,
			Priority:                  3,
		},
		{
			JobId:                     uuid.NewString(),
			Queue:                     "B",
			NegatedPriorityClassValue: -math.MaxInt,
			Priority:                  1,
		},
		{
			JobId:                     uuid.NewString(),
			Queue:                     "B",
			NegatedPriorityClassValue: -math.MaxInt,
			Priority:                  2,
		},
		{
			JobId:                     uuid.NewString(),
			Queue:                     "B",
			NegatedPriorityClassValue: -math.MaxInt + 1,
			Priority:                  1,
		},
		{
			JobId:                     uuid.NewString(),
			Queue:                     "B",
			NegatedPriorityClassValue: 0,
			Priority:                  0,
		},
		{
			JobId:                     uuid.NewString(),
			Queue:                     "B",
			NegatedPriorityClassValue: 0,
			Priority:                  1,
		},
		{
			JobId:                     uuid.NewString(),
			Queue:                     "B",
			NegatedPriorityClassValue: math.MaxInt,
			Priority:                  1,
		},
		{
			JobId:                     uuid.NewString(),
			Queue:                     "B",
			NegatedPriorityClassValue: math.MaxInt,
			Priority:                  2,
		},
		{
			JobId:                     uuid.NewString(),
			Queue:                     "C",
			NegatedPriorityClassValue: -10,
			Priority:                  10,
		},
	}
}
