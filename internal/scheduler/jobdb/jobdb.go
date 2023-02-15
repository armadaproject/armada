package jobdb

import (
	"fmt"
	"math"

	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
)

const (
	jobsTable      = "jobs"
	runsByJobTable = "runsByJob" // table that maps runs to jobs
	idIndex        = "id"        // index for looking up jobs by id
	orderIndex     = "order"     // index for looking up jobs on a given queue by the order in which they should be scheduled
	jobIdIndex     = "jobId"     // index for looking up jobs by id
)

// JobDb is the scheduler-internal system for storing job queues.
// It allows for efficiently iterating over jobs in a specified queue sorted first by in-queue priority value (smaller
// to greater, since smaller values indicate higher priority), and second by submission time.
// JobDb is implemented on top of https://github.com/hashicorp/go-memdb which is a simple in-memory database built on
// immutable radix trees.
type JobDb struct {
	// In-memory database. Stores *Job.
	// Used to efficiently iterate over jobs in sorted order.
	Db *memdb.MemDB
}

// Internal row which allows us to map jobs to job runs (and vice versa)
type jobRunPair struct {
	runId string
	jobId string
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

// Upsert will insert the given jobs if they don't already exist or update the if they do
func (jobDb *JobDb) Upsert(txn *memdb.Txn, jobs []*Job) error {
	for _, job := range jobs {
		err := txn.Insert(jobsTable, job)
		if err != nil {
			return errors.WithStack(err)
		}
		for _, run := range job.runsById {
			err := txn.Insert(runsByJobTable, &jobRunPair{
				runId: run.id.String(),
				jobId: job.id,
			})
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

// GetById returns the job with the given Id or nil if no such job exists
// The Job returned by this function *must not* be subsequently modified
func (jobDb *JobDb) GetById(txn *memdb.Txn, id string) (*Job, error) {
	var job *Job = nil
	iter, err := txn.Get(jobsTable, idIndex, id)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	result := iter.Next()
	if result != nil {
		job = result.(*Job)
	}
	return job, err
}

// GetByRunId returns the job with the given run id or nil if no such job exists
// The Job returned by this function *must not* be subsequently modified
func (jobDb *JobDb) GetByRunId(txn *memdb.Txn, runId uuid.UUID) (*Job, error) {
	iter, err := txn.Get(runsByJobTable, idIndex, runId.String())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	result := iter.Next()
	if result == nil {
		return nil, nil
	}
	job := result.(*jobRunPair)
	return jobDb.GetById(txn, job.jobId)
}

// HasQueuedJobs returns true if the queue has any jobs in the running state or false otherwise
func (jobDb *JobDb) HasQueuedJobs(txn *memdb.Txn, queue string) (bool, error) {
	iter, err := NewJobQueueIterator(txn, queue)
	if err != nil {
		return false, err
	}
	return iter.Next() != nil, nil
}

// GetAll returns all jobs in the database.
// The Jobs returned by this function *must not* be subsequently modified
func (jobDb *JobDb) GetAll(txn *memdb.Txn) ([]*Job, error) {
	iter, err := txn.Get(jobsTable, idIndex)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	result := make([]*Job, 0)
	for obj := iter.Next(); obj != nil; obj = iter.Next() {
		p := obj.(*Job)
		result = append(result, p)
	}
	return result, nil
}

// BatchDelete removes the jobs with the given ids from the database.  Any ids that are not in the database will be
// ignored
func (jobDb *JobDb) BatchDelete(txn *memdb.Txn, ids []string) error {
	for _, id := range ids {
		err := txn.Delete(jobsTable, &Job{id: id})
		if err != nil {
			// this could be because the job doesn't exist
			// unfortunately the error from memdb isn't nice for parsing, so we do an explicit check
			job, err := jobDb.GetById(txn, id)
			if err != nil {
				return err
			}
			if job != nil {
				return errors.WithStack(err)
			}
		}
		runsIter, err := txn.Get(runsByJobTable, jobIdIndex, id)
		if err != nil {
			return errors.WithStack(err)
		}
		for obj := runsIter.Next(); obj != nil; obj = runsIter.Next() {
			err = txn.Delete(runsByJobTable, obj)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

// ReadTxn returns a read-only transaction.
// Multiple read-only transactions can access the db concurrently
func (jobDb *JobDb) ReadTxn() *memdb.Txn {
	return jobDb.Db.Txn(false)
}

// WriteTxn returns a writeable transaction.
// Only a single write transaction may access the db at any given time
func (jobDb *JobDb) WriteTxn() *memdb.Txn {
	return jobDb.Db.Txn(true)
}

// JobQueueIterator is an iterator over all jobs in a given queue.
// Jobs are sorted first by per-queue priority, and secondly by submission time.
type JobQueueIterator struct {
	queue string
	it    memdb.ResultIterator
}

func NewJobQueueIterator(txn *memdb.Txn, queue string) (*JobQueueIterator, error) {
	var minPriority uint32 = 0
	minTimestamp := -math.MaxInt64
	it, err := txn.LowerBound(jobsTable, orderIndex, queue, true, minPriority, minTimestamp)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &JobQueueIterator{
		queue: queue,
		it:    it,
	}, nil
}

// WatchCh is needed to implement the memdb.ResultIterator interface but is not needed for our use case
func (it *JobQueueIterator) WatchCh() <-chan struct{} {
	panic("not implemented")
}

// NextJobItem returns the next Job or nil if the end of the iterator has been reached
func (it *JobQueueIterator) NextJobItem() *Job {
	obj := it.it.Next()
	if obj == nil {
		return nil
	}
	jobItem, ok := obj.(*Job)
	if !ok {
		panic(fmt.Sprintf("expected *SchedulerNode, but got %T", obj))
	}
	if jobItem.queue != it.queue {
		// The index is sorted by queue first.
		// So we've seen all jobs in this queue when this comparison fails.
		return nil
	}
	return jobItem
}

// Next is needed to implement the memdb.ResultIterator interface.  External callers should use NextJobItem which
// provides a typesafe mechanism for getting the next Job
func (it *JobQueueIterator) Next() interface{} {
	return it.NextJobItem()
}

// jobDbSchema() creates the database schema.
// This is a simple schema consisting of a single "jobs" table with indexes for fast lookups
func jobDbSchema() *memdb.DBSchema {
	jobIndexes := map[string]*memdb.IndexSchema{
		idIndex: {
			Name:    idIndex, // lookup by primary key
			Unique:  true,
			Indexer: &memdb.StringFieldIndex{Field: "id"},
		},
		orderIndex: {
			Name:   orderIndex, // lookup leased jobs for a given queue
			Unique: false,
			Indexer: &memdb.CompoundIndex{
				Indexes: []memdb.Indexer{
					&memdb.StringFieldIndex{Field: "queue"},
					&memdb.BoolFieldIndex{Field: "queued"},
					&memdb.UintFieldIndex{Field: "priority"},
					&memdb.IntFieldIndex{Field: "created"},
				},
			},
		},
	}

	runsByJobIndexes := map[string]*memdb.IndexSchema{
		idIndex: {
			Name:    idIndex, // lookup by primary key
			Unique:  true,
			Indexer: &memdb.StringFieldIndex{Field: "runId"},
		},
		jobIdIndex: {
			Name:    jobIdIndex, // lookup by job id
			Unique:  true,
			Indexer: &memdb.StringFieldIndex{Field: "jobId"},
		},
	}

	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			jobsTable: {
				Name:    jobsTable,
				Indexes: jobIndexes,
			},
			runsByJobTable: {
				Name:    runsByJobTable,
				Indexes: runsByJobIndexes,
			},
		},
	}
}
