package scheduler

import (
	"fmt"
	"math"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

const (
	jobsTable  = "jobs"
	idIndex    = "id"    // index for looking up jobs by id
	orderIndex = "order" // index for looking up jobs on a given queue by the order in which they should be scheduled
)

// JobDb is the scheduler-internal system for storing job queues.
// It allows for efficiently iterating over jobs in a specified queue sorted first by in-queue priority value (smaller
// to greater, since smaller values indicate higher priority), and second by submission time.
// JobDb is implemented on top of https://github.com/hashicorp/go-memdb which is a simple in-memory database built on
// immutable radix trees.
type JobDb struct {
	// In-memory database. Stores *SchedulerJob.
	// Used to efficiently iterate over jobs in sorted order.
	Db *memdb.MemDB
}

// SchedulerJob is the scheduler-internal representation of a job.
type SchedulerJob struct {
	// String representation of the job id
	JobId string
	// Name of the queue this job belongs to.
	Queue string
	// Jobset the job belongs to
	// We store this so we can send messages about the job
	Jobset string
	// Per-queue priority of this job.
	Priority uint32
	// Logical timestamp indicating the order in which jobs are submitted.
	// Jobs with identical Queue and Priority
	// are sorted by timestamp.
	Timestamp int64
	// Name of the executor to which this job has been assigned.
	// Empty if this job has not yet been assigned.
	Executor string
	// Name of the node to which this job has been assigned.
	// Empty if this job has not yet been assigned.
	Node string
	// True if the job is currently queued.
	// If this is set then the job will not be considered for scheduling
	Queued bool
	// Scheduling requirements of this job.
	jobSchedulingInfo *schedulerobjects.JobSchedulingInfo
	// True if the user has requested this job be cancelled
	CancelRequested bool
	// True if the scheduler has cancelled the job
	Cancelled bool
	// True if the scheduler has failed the job
	Failed bool
	// True if the scheduler has marked the job as succeeded
	Succeeded bool
	// Job Runs in the order they were received.
	// For now there can be only one active job run which will be the last element of the slice
	Runs []*JobRun
}

func (job *SchedulerJob) GetRequirements(_ map[string]configuration.PriorityClass) *schedulerobjects.JobSchedulingInfo {
	return job.jobSchedulingInfo
}

// GetQueue returns the queue this job belongs to.
func (job *SchedulerJob) GetQueue() string {
	return job.Queue
}

// GetAnnotations returns the annotations on the job.
func (job *SchedulerJob) GetAnnotations() map[string]string {
	requirements := job.jobSchedulingInfo.GetObjectRequirements()
	if len(requirements) == 0 {
		return nil
	}
	if podReqs := requirements[0].GetPodRequirements(); podReqs != nil {
		return podReqs.GetAnnotations()
	}
	return nil
}

// GetId returns the id of the Job.
func (job *SchedulerJob) GetId() string {
	return job.JobId
}

// InTerminalState returns true if the job  is in a terminal state
func (job *SchedulerJob) InTerminalState() bool {
	return job.Succeeded || job.Cancelled || job.Failed
}

// NumReturned returns the number of times this job has been returned by executors
// Note that this is O(N) on Runs, but this should be fine as the number of runs should be small
func (job *SchedulerJob) NumReturned() uint {
	returned := uint(0)
	for _, run := range job.Runs {
		if run.Returned {
			returned++
		}
	}
	return returned
}

// CurrentRun returns the currently active job run or nil if there are no runs yet
func (job *SchedulerJob) CurrentRun() *JobRun {
	if len(job.Runs) == 0 {
		return nil
	}
	return job.Runs[len(job.Runs)-1]
}

// RunById returns the Run corresponding to the provided run id or nil if no such Run exists
// Note that this is O(N) on Runs, but this should be fine as the number of runs should be small
func (job *SchedulerJob) RunById(id uuid.UUID) *JobRun {
	for _, run := range job.Runs {
		if run.RunID == id {
			return run
		}
	}
	return nil
}

// DeepCopy deep copies the entire job including the runs.
// This is needed because when jobs are stored in the JobDb they cannot be modified in-place
func (job *SchedulerJob) DeepCopy() *SchedulerJob {
	if job == nil {
		return nil
	}
	runs := make([]*JobRun, len(job.Runs))
	for k, v := range job.Runs {
		runs[k] = v.DeepCopy()
	}
	return &SchedulerJob{
		JobId:             job.JobId,
		Queue:             job.Queue,
		Jobset:            job.Jobset,
		Priority:          job.Priority,
		Timestamp:         job.Timestamp,
		Node:              job.Node,
		Queued:            job.Queued,
		jobSchedulingInfo: proto.Clone(job.jobSchedulingInfo).(*schedulerobjects.JobSchedulingInfo),
		CancelRequested:   job.CancelRequested,
		Cancelled:         job.Cancelled,
		Failed:            job.Failed,
		Succeeded:         job.Succeeded,
		Runs:              runs,
	}
}

// JobRun is the scheduler-internal representation of a job run.
type JobRun struct {
	// Unique identifier for the run
	RunID uuid.UUID
	// The name of the executor this run has been leased to
	Executor string
	// True if the job has been reported as pending by the executor
	Pending bool
	// True if the job has been reported as running by the executor
	Running bool
	// True if the job has been reported as succeeded by the executor
	Succeeded bool
	// True if the job has been reported as failed by the executor
	Failed bool
	// True if the job has been reported as cancelled by the executor
	Cancelled bool
	// True if the job has been returned by the executor
	Returned bool
	// True if the job has been expired by the scheduler
	Expired bool
}

// InTerminalState returns true if the JobRun is in a terminal state
func (run *JobRun) InTerminalState() bool {
	return run.Succeeded || run.Failed || run.Cancelled || run.Expired || run.Returned
}

// DeepCopy deep copies the entire JobRun
// This is needed because when runs are stored in the JobDb they cannot be modified in-place
func (run *JobRun) DeepCopy() *JobRun {
	return &JobRun{
		RunID:     run.RunID,
		Executor:  run.Executor,
		Pending:   run.Pending,
		Running:   run.Running,
		Succeeded: run.Succeeded,
		Failed:    run.Failed,
		Cancelled: run.Cancelled,
		Returned:  run.Returned,
		Expired:   run.Expired,
	}
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
// Any jobs passed to this function *must not* be subsequently modified
func (jobDb *JobDb) Upsert(txn *memdb.Txn, jobs []*SchedulerJob) error {
	for _, job := range jobs {
		err := txn.Insert(jobsTable, job)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// GetById returns the job with the given Id or nil if no such job exists
// The Job returned by this function *must not* be subsequently modified
func (jobDb *JobDb) GetById(txn *memdb.Txn, id string) (*SchedulerJob, error) {
	var job *SchedulerJob = nil
	iter, err := txn.Get(jobsTable, idIndex, id)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	result := iter.Next()
	if result != nil {
		job = result.(*SchedulerJob)
	}
	return job, err
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
func (jobDb *JobDb) GetAll(txn *memdb.Txn) ([]*SchedulerJob, error) {
	iter, err := txn.Get(jobsTable, idIndex)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	result := make([]*SchedulerJob, 0)
	for obj := iter.Next(); obj != nil; obj = iter.Next() {
		p := obj.(*SchedulerJob)
		result = append(result, p)
	}
	return result, nil
}

// BatchDelete removes the jobs with the given ids from the database.  Any ids that are not in the database will be
// ignored
func (jobDb *JobDb) BatchDelete(txn *memdb.Txn, ids []string) error {
	for _, id := range ids {
		err := txn.Delete(jobsTable, &SchedulerJob{JobId: id})
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

// NextJobItem returns the next SchedulerJob or nil if the end of the iterator has been reached
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
		// The index is sorted by queue first.
		// So we've seen all jobs in this queue when this comparison fails.
		return nil
	}
	return jobItem
}

// Next is needed to implement the memdb.ResultIterator interface.  External callers should use NextJobItem which
// provides a typesafe mechanism for getting the next SchedulerJob
func (it *JobQueueIterator) Next() interface{} {
	return it.NextJobItem()
}

// jobDbSchema() creates the database schema.
// This is a simple schema consisting of a single "jobs" table with indexes for fast lookups
func jobDbSchema() *memdb.DBSchema {
	indexes := make(map[string]*memdb.IndexSchema)
	indexes[idIndex] = &memdb.IndexSchema{
		Name:    idIndex, // lookup by primary key
		Unique:  true,
		Indexer: &memdb.StringFieldIndex{Field: "JobId"},
	}
	indexes[orderIndex] = &memdb.IndexSchema{
		Name:   orderIndex, // lookup leased jobs for a given queue
		Unique: false,
		Indexer: &memdb.CompoundIndex{
			Indexes: []memdb.Indexer{
				&memdb.StringFieldIndex{Field: "Queue"},
				&memdb.BoolFieldIndex{Field: "Queued"},
				&memdb.UintFieldIndex{Field: "Priority"},
				&memdb.IntFieldIndex{Field: "Timestamp"},
			},
		},
	}
	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			jobsTable: {
				Name:    jobsTable,
				Indexes: indexes,
			},
		},
	}
}
