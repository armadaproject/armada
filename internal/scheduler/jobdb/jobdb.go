package jobdb

import (
	"sync"

	"github.com/benbjohnson/immutable"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
)

var emptyList = immutable.NewSortedSet[*Job](JobPriorityComparer{})

type JobDb struct {
	jobsById    *immutable.Map[string, *Job]
	jobsByRunId *immutable.Map[uuid.UUID, string]
	jobsByQueue map[string]immutable.SortedSet[*Job]
	copyMutex   sync.Mutex
	writerMutex sync.Mutex
}

func NewJobDb() *JobDb {
	return &JobDb{
		jobsById:    immutable.NewMap[string, *Job](nil),
		jobsByRunId: immutable.NewMap[uuid.UUID, string](&UUIDHasher{}),
		jobsByQueue: map[string]immutable.SortedSet[*Job]{},
		copyMutex:   sync.Mutex{},
	}
}

// Upsert will insert the given jobs if they don't already exist or update the if they do
func (jobDb *JobDb) Upsert(txn *Txn, jobs []*Job) error {
	if err := jobDb.checkWritableTransaction(txn); err != nil {
		return err
	}

	hasJobs := txn.jobsById.Len() > 0

	// First we need to delete the state of any queued jobs
	if hasJobs {
		for _, job := range jobs {
			existingJob, ok := txn.jobsById.Get(job.id)
			if ok {
				existingQueue, ok := txn.jobsByQueue[existingJob.queue]
				if ok {
					txn.jobsByQueue[existingJob.queue] = existingQueue.Delete(existingJob)
				}
			}
		}
	}

	// Now need to insert jobs, runs and queuedJobs.  This can be done in parallel
	wg := sync.WaitGroup{}
	wg.Add(3)

	// jobs
	go func() {
		defer wg.Done()
		if hasJobs {
			for _, job := range jobs {
				txn.jobsById = txn.jobsById.Set(job.id, job)
			}
		} else {
			jobsById := immutable.NewMapBuilder[string, *Job](nil)
			for _, job := range jobs {
				jobsById.Set(job.id, job)
			}
			txn.jobsById = jobsById.Map()
		}
	}()

	// runs
	go func() {
		defer wg.Done()
		if hasJobs {
			for _, job := range jobs {
				for _, run := range job.runsById {
					txn.jobsByRunId = txn.jobsByRunId.Set(run.id, job.id)
				}
			}
		} else {
			jobsByRunId := immutable.NewMapBuilder[uuid.UUID, string](&UUIDHasher{})
			for _, job := range jobs {
				for _, run := range job.runsById {
					jobsByRunId.Set(run.id, job.id)
				}
			}
			txn.jobsByRunId = jobsByRunId.Map()
		}
	}()

	// queued Jobs
	go func() {
		defer wg.Done()
		for _, job := range jobs {
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
	}()
	wg.Wait()
	return nil
}

// GetById returns the job with the given Id or nil if no such job exists
// The Job returned by this function *must not* be subsequently modified
func (jobDb *JobDb) GetById(txn *Txn, id string) *Job {
	j, _ := txn.jobsById.Get(id)
	return j
}

// GetByRunId returns the job with the given run id or nil if no such job exists
// The Job returned by this function *must not* be subsequently modified
func (jobDb *JobDb) GetByRunId(txn *Txn, runId uuid.UUID) *Job {
	jobId, _ := txn.jobsByRunId.Get(runId)
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
	allJobs := make([]*Job, 0, txn.jobsById.Len())
	iter := txn.jobsById.Iterator()
	for !iter.Done() {
		_, job, _ := iter.Next()
		allJobs = append(allJobs, job)
	}
	return allJobs
}

// BatchDelete deletes the jobs with the given ids from the database.
// Any ids not in the database are ignored.
func (jobDb *JobDb) BatchDelete(txn *Txn, ids []string) error {
	if err := jobDb.checkWritableTransaction(txn); err != nil {
		return err
	}
	for _, id := range ids {
		job, present := txn.jobsById.Get(id)
		if present {
			txn.jobsById = txn.jobsById.Delete(id)
			for _, run := range job.runsById {
				txn.jobsByRunId = txn.jobsByRunId.Delete(run.id)
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

func (jobDb *JobDb) checkWritableTransaction(txn *Txn) error {
	if txn.readOnly {
		return errors.New("Cannot write using a read only transaction")
	}
	if !txn.active {
		return errors.New("Cannot write using an inactive transaction")
	}
	return nil
}

// ReadTxn returns a read-only transaction.
// Multiple read-only transactions can access the db concurrently
func (jobDb *JobDb) ReadTxn() *Txn {
	jobDb.copyMutex.Lock()
	defer jobDb.copyMutex.Unlock()
	return &Txn{
		readOnly:    true,
		jobsById:    jobDb.jobsById,
		jobsByRunId: jobDb.jobsByRunId,
		jobsByQueue: jobDb.jobsByQueue,
		active:      true,
		jobDb:       jobDb,
	}
}

// WriteTxn returns a writeable transaction.
// Only a single write transaction may access the db at any given time so note that this function will block until
// any outstanding write transactions  have been committed or aborted
func (jobDb *JobDb) WriteTxn() *Txn {
	jobDb.writerMutex.Lock()
	jobDb.copyMutex.Lock()
	defer jobDb.copyMutex.Unlock()
	return &Txn{
		readOnly:    false,
		jobsById:    jobDb.jobsById,
		jobsByRunId: jobDb.jobsByRunId,
		jobsByQueue: maps.Clone(jobDb.jobsByQueue),
		active:      true,
		jobDb:       jobDb,
	}
}

// Txn is a JobDb Transaction. Transactions provide a consistent view of the database, allowing readers to
// perform multiple actions without the database changing from underneath them.
// Write transactions also allow callers to perform write operations that will not be visible to other users
// until the transaction is committed.
type Txn struct {
	readOnly    bool
	jobsById    *immutable.Map[string, *Job]
	jobsByRunId *immutable.Map[uuid.UUID, string]
	jobsByQueue map[string]immutable.SortedSet[*Job]
	jobDb       *JobDb
	active      bool
}

func (txn *Txn) Commit() {
	if txn.readOnly || !txn.active {
		return
	}
	txn.jobDb.copyMutex.Lock()
	defer txn.jobDb.copyMutex.Unlock()
	defer txn.jobDb.writerMutex.Unlock()
	txn.jobDb.jobsById = txn.jobsById
	txn.jobDb.jobsByRunId = txn.jobsByRunId
	txn.jobDb.jobsByQueue = txn.jobsByQueue
	txn.active = false
}

func (txn *Txn) Abort() {
	if txn.readOnly || !txn.active {
		return
	}
	txn.active = false
	txn.jobDb.writerMutex.Unlock()
}
