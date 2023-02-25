package jobdb

import (
	"github.com/benbjohnson/immutable"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"sync"
)

var emptyList = immutable.NewSortedSet[*Job](JobPriorityComparer{})

type JobDb struct {
	jobsById    map[string]*Job
	jobsByRunId map[uuid.UUID]string
	jobsByQueue map[string]immutable.SortedSet[*Job]
	mutex       sync.Mutex
}

func NewJobDb() *JobDb {
	return &JobDb{
		jobsById:    map[string]*Job{},
		jobsByRunId: map[uuid.UUID]string{},
		jobsByQueue: map[string]immutable.SortedSet[*Job]{},
		mutex:       sync.Mutex{},
	}
}

// Upsert will insert the given jobs if they don't already exist or update the if they do
func (jobDb *JobDb) Upsert(txn *Txn, jobs []*Job) error {
	if txn.readOnly {
		return errors.New("Cannot upsert using a read only transaction")
	}
	for _, job := range jobs {
		existingJob := txn.jobsById[job.id]
		if existingJob != nil {
			existingQueue, ok := txn.jobsByQueue[existingJob.queue]
			if ok {
				txn.jobsByQueue[existingJob.queue] = existingQueue.Delete(existingJob)
			}
		}
		txn.jobsById[job.id] = job
		for _, run := range job.runsById {
			txn.jobsByRunId[run.id] = job.id
		}
		if job.Queued() {
			newQueue, ok := txn.jobsByQueue[job.queue]
			if !ok {
				q := emptyList
				newQueue = q
			}
			newQueue = newQueue.Add(job)
			txn.jobsByQueue[job.queue] = newQueue
		}
	}
	return nil
}

// GetById returns the job with the given Id or nil if no such job exists
// The Job returned by this function *must not* be subsequently modified
func (jobDb *JobDb) GetById(txn *Txn, id string) *Job {
	return txn.jobsById[id]
}

// GetByRunId returns the job with the given run id or nil if no such job exists
// The Job returned by this function *must not* be subsequently modified
func (jobDb *JobDb) GetByRunId(txn *Txn, runId uuid.UUID) *Job {
	jobId := txn.jobsByRunId[runId]
	return jobDb.GetById(txn, jobId)
}

// HasQueuedJobs returns true if the queue has any jobs in the running state or false otherwise
func (jobDb *JobDb) HasQueuedJobs(txn *Txn, queue string) bool {
	queuedJobs, ok := txn.jobsByQueue[queue]
	if !ok {
		return false
	}
	return queuedJobs.Len() > 0
}

// QueuedJobs returns true if the queue has any jobs in the running state or false otherwise
func (jobDb *JobDb) QueuedJobs(txn *Txn, queue string) *immutable.SortedSetIterator[*Job] {
	jobQueue, ok := txn.jobsByQueue[queue]
	if ok {
		return jobQueue.Iterator()
	} else {
		return emptyList.Iterator()
	}
}

// GetAll returns all jobs in the database.
// The Jobs returned by this function *must not* be subsequently modified
func (jobDb *JobDb) GetAll(txn *Txn) []*Job {
	return maps.Values(txn.jobsById)
}

// BatchDelete removes the jobs with the given ids from the database.  Any ids that are not in the database will be
// ignored
func (jobDb *JobDb) BatchDelete(txn *Txn, ids []string) error {
	if txn.readOnly {
		return errors.New("Cannot delete using a read only transaction")
	}
	for _, id := range ids {
		job, present := txn.jobsById[id]
		if present {
			delete(txn.jobsById, id)
			for _, run := range job.runsById {
				delete(txn.jobsByRunId, run.id)
			}
			queue, ok := txn.jobsByQueue[job.queue]
			if ok {
				newQueue := queue.Delete(job)
				txn.jobsByQueue[job.queue] = newQueue
			}
		}
	}
	return nil
}

// ReadTxn returns a read-only transaction.
// Multiple read-only transactions can access the db concurrently
func (jobDb *JobDb) ReadTxn() *Txn {
	jobDb.mutex.Lock()
	defer jobDb.mutex.Unlock()
	return &Txn{
		readOnly:    true,
		jobsById:    jobDb.jobsById,
		jobsByRunId: jobDb.jobsByRunId,
		jobsByQueue: jobDb.jobsByQueue,
		jobDb:       jobDb,
	}
}

// WriteTxn returns a writeable transaction.
// Only a single write transaction may access the db at any given time
func (jobDb *JobDb) WriteTxn() *Txn {
	jobDb.mutex.Lock()
	defer jobDb.mutex.Unlock()
	return &Txn{
		readOnly:    false,
		jobsById:    maps.Clone(jobDb.jobsById),
		jobsByRunId: maps.Clone(jobDb.jobsByRunId),
		jobsByQueue: maps.Clone(jobDb.jobsByQueue),
		jobDb:       jobDb,
	}
}

type Txn struct {
	readOnly    bool
	jobsById    map[string]*Job
	jobsByRunId map[uuid.UUID]string
	jobsByQueue map[string]immutable.SortedSet[*Job]
	jobDb       *JobDb
}

func (txn *Txn) Commit() {
	txn.jobDb.mutex.Lock()
	defer txn.jobDb.mutex.Unlock()
	txn.jobDb.jobsById = txn.jobsById
	txn.jobDb.jobsByRunId = txn.jobsByRunId
	txn.jobDb.jobsByQueue = txn.jobsByQueue
}

func (txn *Txn) Abort() {}
