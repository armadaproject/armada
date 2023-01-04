package scheduler

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
)

func TestJobDbSchema(t *testing.T) {
	err := jobDbSchema().Validate()
	assert.NoError(t, err)
}

func TestDelete(t *testing.T) {
	db, err := NewJobDb()
	if !assert.NoError(t, err) {
		return
	}
	items := testJobItems1()
	txn := db.WriteTxn()
	err = db.Upsert(txn, items)
	assert.NoError(t, err)

	job, err := db.GetById(txn, items[0].JobId)
	assert.NoError(t, err)
	assert.Equal(t, items[0], job)

	err = db.BatchDelete(txn, []string{items[0].JobId})
	assert.NoError(t, err)

	job, err = db.GetById(txn, items[0].JobId)
	assert.NoError(t, err)
	assert.Nil(t, job)
}

func TestPerformance(t *testing.T) {
	db, err := NewJobDb()
	if !assert.NoError(t, err) {
		return
	}

	numItems := 1000000
	items := make([]*SchedulerJob, numItems)
	for i := 0; i < numItems; i++ {
		items[i] = &SchedulerJob{
			JobId:             uuid.NewString(),
			Queue:             "A",
			Priority:          0,
			Timestamp:         10,
			Node:              "",
			jobSchedulingInfo: nil,
		}
	}
	startInsert := time.Now()
	txn := db.WriteTxn()
	err = db.Upsert(txn, items)
	assert.NoError(t, err)
	taken := time.Now().Sub(startInsert).Milliseconds()
	println(fmt.Sprintf("Inserted in %dms", taken))
	numDeletes := 100000
	ids := make([]string, numDeletes)
	for i := 0; i < numDeletes; i++ {
		ids[i] = items[i].JobId
	}
	startDelete := time.Now()
	err = db.BatchDelete(txn, ids)
	assert.NoError(t, err)
	taken = time.Now().Sub(startDelete).Milliseconds()
	println(fmt.Sprintf("Deleted in %dms", taken))
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
			txn := db.WriteTxn()
			if !assert.NoError(t, err) {
				return
			}

			// Shuffle and insert jobs.
			items := slices.Clone(tc.Items)
			slices.SortFunc(items, func(a, b *SchedulerJob) bool { return rand.Float64() < 0.5 })
			err = db.Upsert(txn, items)
			if !assert.NoError(t, err) {
				return
			}

			// Test that jobs are returned in the expected order.
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
			JobId:             uuid.NewString(),
			Queue:             "A",
			Priority:          0,
			Timestamp:         10,
			Node:              "",
			jobSchedulingInfo: nil,
		},
		{
			JobId:     uuid.NewString(),
			Queue:     "A",
			Priority:  1,
			Timestamp: 0,
		},
		{
			JobId:     uuid.NewString(),
			Queue:     "A",
			Priority:  1,
			Timestamp: 1,
		},
		{
			JobId:    uuid.NewString(),
			Queue:    "A",
			Priority: 0,
		},
		{
			JobId:    uuid.NewString(),
			Queue:    "A",
			Priority: 1,
		},
		{
			JobId:    uuid.NewString(),
			Queue:    "A",
			Priority: 3,
		},
		{
			JobId:    uuid.NewString(),
			Queue:    "B",
			Priority: 1,
		},
		{
			JobId:    uuid.NewString(),
			Queue:    "B",
			Priority: 2,
		},
		{
			JobId:    uuid.NewString(),
			Queue:    "B",
			Priority: 1,
		},
		{
			JobId:    uuid.NewString(),
			Queue:    "B",
			Priority: 0,
		},
		{
			JobId:    uuid.NewString(),
			Queue:    "B",
			Priority: 1,
		},
		{
			JobId:    uuid.NewString(),
			Queue:    "B",
			Priority: 1,
		},
		{
			JobId:    uuid.NewString(),
			Queue:    "B",
			Priority: 2,
		},
		{
			JobId:    uuid.NewString(),
			Queue:    "C",
			Priority: 10,
		},
	}
}
