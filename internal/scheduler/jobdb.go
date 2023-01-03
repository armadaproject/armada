package scheduler

import (
	"fmt"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"math"
)

const (
	jobsTable  = "jobs"
	idIndex    = "id"
	orderIndex = "order"
)

// JobDb is the scheduler-internal system for storing job queues.
// It allows for efficiently iterating over jobs in a specified queue sorted
// first by priority class value (greater to smaller),
// second by in-queue priority value (smaller to greater, since smaller values indicate higher priority),
// and third by submission time.
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
	// Jobset the job belongs to
	// We store this so we can send messages about the job
	Jobset string
	// Per-queue priority of this job.
	Priority uint32
	// Logical timestamp indicating the order in which jobs are submitted.
	// Jobs with identical queue and Priority
	// are sorted by timestamp.
	Timestamp int64
	// Name of the executor to which this job has been assigned.
	// Empty if this job has not yet been assigned.
	Executor string
	// Name of the node to which this job has been assigned.
	// Empty if this job has not yet been assigned.
	Node string
	// True if the job  is currently Leased.
	// If this is set then the job will not be considered for scheduling
	Leased bool
	// Scheduling requirements of this job.
	jobSchedulingInfo *schedulerobjects.JobSchedulingInfo
	CancelRequested   bool
	Cancelled         bool
	Failed            bool
	Succeeded         bool
	// Job Runs in the order they were received
	Runs []*JobRun
}

func (job *SchedulerJob) GetQueue() string {
	return job.Queue
}

func (job *SchedulerJob) GetAnnotations() map[string]string {
	return job.GetAnnotations()
}

func (job *SchedulerJob) GetId() string {
	return job.JobId
}

func (job *SchedulerJob) NumReturned() uint {
	returned := uint(0)
	for _, run := range job.Runs {
		if run.Returned {
			returned++
		}
	}
	return returned
}

func (job *SchedulerJob) CurrentRun() *JobRun {
	if len(job.Runs) == 0 {
		return nil
	}
	return job.Runs[len(job.Runs)-1]
}

func (job *SchedulerJob) RunById(id uuid.UUID) (*JobRun, bool) {
	for _, run := range job.Runs {
		if run.RunID == id {
			return run, true
		}
	}
	return nil, false
}

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
		Leased:            job.Leased,
		jobSchedulingInfo: proto.Clone(job.jobSchedulingInfo).(*schedulerobjects.JobSchedulingInfo),
		CancelRequested:   job.CancelRequested,
		Cancelled:         job.Cancelled,
		Failed:            job.Failed,
		Succeeded:         job.Succeeded,
		Runs:              runs,
	}
}

type JobRun struct {
	RunID     uuid.UUID
	Executor  string
	Pending   bool
	Running   bool
	Succeeded bool
	Failed    bool
	Cancelled bool
	Returned  bool
	Expired   bool
}

func (run *JobRun) Terminal() bool {
	return run.Succeeded || run.Failed || run.Cancelled || run.Expired || run.Returned
}

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

func (jobDb *JobDb) Upsert(txn *memdb.Txn, jobs []*SchedulerJob) error {
	for _, job := range jobs {
		err := txn.Insert(jobsTable, job)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

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

func (jobDb *JobDb) BatchDeleteById(txn *memdb.Txn, ids []string) error {
	for _, id := range ids {
		err := txn.Delete(jobsTable, &SchedulerJob{JobId: id})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (jobDb *JobDb) ReadTxn() *memdb.Txn {
	return jobDb.Db.Txn(false)
}

func (jobDb *JobDb) WriteTxn() *memdb.Txn {
	return jobDb.Db.Txn(true)
}

// JobQueueIterator is an iterator over all jobs in a given queue.
// Jobs are sorted first by PriorityClassValue, second by per-queue priority, and third by submission time.
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
	indexes[idIndex] = &memdb.IndexSchema{
		Name:    idIndex,
		Unique:  true,
		Indexer: &memdb.StringFieldIndex{Field: "JobId"},
	}
	indexes[orderIndex] = &memdb.IndexSchema{
		Name:   orderIndex,
		Unique: false,
		Indexer: &memdb.CompoundIndex{
			Indexes: []memdb.Indexer{
				&memdb.StringFieldIndex{Field: "Queue"},
				&memdb.BoolFieldIndex{Field: "Leased"},
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
