package scheduler

import (
	"fmt"
	"math"

	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
)

// JobDb is the scheduler-internal system for storing job queues.
// It allows for efficiently iterationg over jobs in a specified queue sorted
// first by priority class value (greater to smaller) and
// second by in-queue priority value (smaller to greater, since smaller values indicate higher priority).
type JobDb struct {
	// In-memory database. Stores *SchedulerJob.
	// Used to efficiently iterate over jobs in sorted order.
	Db *memdb.MemDB
}

// SchedulerJob is the scheduler-internal representation of a job.
type SchedulerJob struct {
	// String representation of the job id UUID.
	// We use a string rather than a uuid.UUID because go-memdb only
	// supports the string representation natively.
	JobId string
	// Name of the queue this job is part of.
	Queue string
	// Each job has a priority class with an associated value.
	// We store the negative (i.e., multiplied by -1) of this value such
	// that the index is sorted from highest to lowest priority.
	NegatedPriorityClassValue int
	// Per-queue priority of this job.
	Priority uint32
	// Node to which this job has been assigned.
	// Nil if this job has not yet been assigned.
	node *schedulerobjects.Node
	// Scheduling requirements of this job.
	jobSchedulingInfo *schedulerobjects.JobSchedulingInfo
}

func NewJobDb() (*JobDb, error) {
	db, err := memdb.NewMemDB(jobDbSchema())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &JobDb{
		Db: db,
	}, nil
}

func (jobDb *JobDb) Upsert(jobs []*SchedulerJob) error {
	txn := jobDb.Db.Txn(true)
	defer txn.Abort()
	for _, job := range jobs {
		err := txn.Insert("jobs", job)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	txn.Commit()
	return nil
}

// JobQueueIterator is an iterator over all jobs in a given queue.
// Jobs are sorted first by PriorityClassValue and second by per-queue priority.
type JobQueueIterator struct {
	queue string
	it    memdb.ResultIterator
}

func NewJobQueueIterator(txn *memdb.Txn, queue string) (*JobQueueIterator, error) {
	indexName := "order"
	minPriorityClassValue := -math.MaxInt
	var minPriority uint32 = 0
	it, err := txn.LowerBound("jobs", indexName, queue, minPriorityClassValue, minPriority)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &JobQueueIterator{
		queue: queue,
		it:    it,
	}, nil
}

func (it *JobQueueIterator) WatchCh() <-chan struct{} {
	panic("not implemented")
}

func (it *JobQueueIterator) NextJobItem() *SchedulerJob {
	obj := it.it.Next()
	if obj == nil {
		return nil
	}
	jobItem, ok := obj.(*SchedulerJob)
	if !ok {
		panic(fmt.Sprintf("expected *SchedulerNode, but got %T", obj))
	}
	if jobItem.Queue != it.queue {
		// The index is sorted by Queue first.
		// So we've seen all jobs in this queue when this comparison fails.
		return nil
	}
	return jobItem
}

func (it *JobQueueIterator) Next() interface{} {
	return it.NextJobItem()
}

func jobDbSchema() *memdb.DBSchema {
	indexes := make(map[string]*memdb.IndexSchema)
	indexes["id"] = &memdb.IndexSchema{
		Name:    "id",
		Unique:  true,
		Indexer: &memdb.UUIDFieldIndex{Field: "JobId"},
	}
	indexes["order"] = &memdb.IndexSchema{
		Name:   "order",
		Unique: false,
		Indexer: &memdb.CompoundIndex{
			Indexes: []memdb.Indexer{
				&memdb.StringFieldIndex{Field: "Queue"},
				&memdb.IntFieldIndex{Field: "NegatedPriorityClassValue"},
				&memdb.UintFieldIndex{Field: "Priority"},
			},
		},
	}
	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"jobs": {
				Name:    "jobs",
				Indexes: indexes,
			},
		},
	}
}
