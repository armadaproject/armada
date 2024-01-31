package jobdb

import (
	"fmt"
	"sync"

	"github.com/benbjohnson/immutable"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

var (
	emptyList            = immutable.NewSortedSet[*Job](JobPriorityComparer{})
	emptyQueuedJobsByTtl = immutable.NewSortedSet[*Job](JobQueueTtlComparer{})
)

type JobDb struct {
	jobsById        *immutable.Map[string, *Job]
	jobsByRunId     *immutable.Map[uuid.UUID, string]
	jobsByQueue     map[string]immutable.SortedSet[*Job]
	queuedJobsByTtl *immutable.SortedSet[*Job]
	// Configured priority classes.
	priorityClasses map[string]types.PriorityClass
	// Priority class assigned to jobs with a priorityClassName not in jobDb.priorityClasses.
	defaultPriorityClass   types.PriorityClass
	schedulingKeyGenerator *schedulerobjects.SchedulingKeyGenerator
	// We intern strings to save memory.
	stringInterner *stringinterner.StringInterner
	// Mutexes protecting the jobDb.
	copyMutex   sync.Mutex
	writerMutex sync.Mutex
	// Clock used when assigning timestamps to created job runs.
	// Set here so that it can be mocked.
	clock clock.PassiveClock
	// Used for generating job run ids.
	uuidProvider UUIDProvider
}

// UUIDProvider is an interface used to mock UUID generation for tests.
type UUIDProvider interface {
	New() uuid.UUID
}

// RealUUIDProvider calls uuid.New.
type RealUUIDProvider struct{}

func (_ RealUUIDProvider) New() uuid.UUID {
	return uuid.New()
}

func NewJobDb(priorityClasses map[string]types.PriorityClass, defaultPriorityClassName string, stringInternerCacheSize uint32) *JobDb {
	return NewJobDbWithSchedulingKeyGenerator(
		priorityClasses,
		defaultPriorityClassName,
		schedulerobjects.NewSchedulingKeyGenerator(),
		stringInternerCacheSize,
	)
}

func NewJobDbWithSchedulingKeyGenerator(
	priorityClasses map[string]types.PriorityClass,
	defaultPriorityClassName string,
	skg *schedulerobjects.SchedulingKeyGenerator,
	stringInternerCacheSize uint32,
) *JobDb {
	defaultPriorityClass, ok := priorityClasses[defaultPriorityClassName]
	if !ok {
		// TODO(albin): Return an error instead.
		panic(fmt.Sprintf("unknown default priority class %s", defaultPriorityClassName))
	}
	return &JobDb{
		jobsById:               immutable.NewMap[string, *Job](nil),
		jobsByRunId:            immutable.NewMap[uuid.UUID, string](&UUIDHasher{}),
		jobsByQueue:            map[string]immutable.SortedSet[*Job]{},
		queuedJobsByTtl:        &emptyQueuedJobsByTtl,
		priorityClasses:        priorityClasses,
		defaultPriorityClass:   defaultPriorityClass,
		schedulingKeyGenerator: skg,
		stringInterner:         stringinterner.New(stringInternerCacheSize),
		clock:                  clock.RealClock{},
		uuidProvider:           RealUUIDProvider{},
	}
}

func (jobDb *JobDb) SetClock(clock clock.PassiveClock) {
	jobDb.clock = clock
}

func (jobDb *JobDb) SetUUIDProvider(uuidProvider UUIDProvider) {
	jobDb.uuidProvider = uuidProvider
}

// Clone returns a copy of the jobDb.
func (jobDb *JobDb) Clone() *JobDb {
	return &JobDb{
		jobsById:               jobDb.jobsById,
		jobsByRunId:            jobDb.jobsByRunId,
		jobsByQueue:            maps.Clone(jobDb.jobsByQueue),
		queuedJobsByTtl:        jobDb.queuedJobsByTtl,
		priorityClasses:        jobDb.priorityClasses,
		defaultPriorityClass:   jobDb.defaultPriorityClass,
		schedulingKeyGenerator: jobDb.schedulingKeyGenerator,
		stringInterner:         jobDb.stringInterner,
	}
}

// NewJob creates a new scheduler job.
// The new job is not automatically inserted into the jobDb; call jobDb.Upsert to upsert it.
func (jobDb *JobDb) NewJob(
	jobId string,
	jobSet string,
	queue string,
	priority uint32,
	schedulingInfo *schedulerobjects.JobSchedulingInfo,
	queued bool,
	queuedVersion int32,
	cancelRequested bool,
	cancelByJobSetRequested bool,
	cancelled bool,
	created int64,
) *Job {
	priorityClass, ok := jobDb.priorityClasses[schedulingInfo.PriorityClassName]
	if !ok {
		priorityClass = jobDb.defaultPriorityClass
	}
	job := &Job{
		jobDb:                   jobDb,
		id:                      jobId,
		queue:                   jobDb.stringInterner.Intern(queue),
		jobSet:                  jobDb.stringInterner.Intern(jobSet),
		priority:                priority,
		queued:                  queued,
		queuedVersion:           queuedVersion,
		requestedPriority:       priority,
		submittedTime:           created,
		jobSchedulingInfo:       jobDb.internJobSchedulingInfoStrings(schedulingInfo),
		priorityClass:           priorityClass,
		cancelRequested:         cancelRequested,
		cancelByJobSetRequested: cancelByJobSetRequested,
		cancelled:               cancelled,
		runsById:                map[uuid.UUID]*JobRun{},
	}
	job.ensureJobSchedulingInfoFieldsInitialised()
	job.schedulingKey = interfaces.SchedulingKeyFromLegacySchedulerJob(jobDb.schedulingKeyGenerator, job)
	return job
}

func (jobDb *JobDb) internJobSchedulingInfoStrings(info *schedulerobjects.JobSchedulingInfo) *schedulerobjects.JobSchedulingInfo {
	for _, requirement := range info.ObjectRequirements {
		if podRequirement := requirement.GetPodRequirements(); podRequirement != nil {
			for k, v := range podRequirement.Annotations {
				podRequirement.Annotations[jobDb.stringInterner.Intern(k)] = jobDb.stringInterner.Intern(v)
			}

			for k, v := range podRequirement.NodeSelector {
				podRequirement.NodeSelector[jobDb.stringInterner.Intern(k)] = jobDb.stringInterner.Intern(v)
			}
			podRequirement.PreemptionPolicy = jobDb.stringInterner.Intern(podRequirement.PreemptionPolicy)
		}
	}
	return info
}

// ReadTxn returns a read-only transaction.
// Multiple read-only transactions can access the db concurrently
func (jobDb *JobDb) ReadTxn() *Txn {
	jobDb.copyMutex.Lock()
	defer jobDb.copyMutex.Unlock()
	return &Txn{
		readOnly:        true,
		jobsById:        jobDb.jobsById,
		jobsByRunId:     jobDb.jobsByRunId,
		jobsByQueue:     jobDb.jobsByQueue,
		queuedJobsByTtl: jobDb.queuedJobsByTtl,
		active:          true,
		jobDb:           jobDb,
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
		readOnly:        false,
		jobsById:        jobDb.jobsById,
		jobsByRunId:     jobDb.jobsByRunId,
		jobsByQueue:     maps.Clone(jobDb.jobsByQueue),
		queuedJobsByTtl: jobDb.queuedJobsByTtl,
		active:          true,
		jobDb:           jobDb,
	}
}

// Txn is a JobDb Transaction. Transactions provide a consistent view of the database, allowing readers to
// perform multiple actions without the database changing from underneath them.
// Write transactions also allow callers to perform write operations that will not be visible to other users
// until the transaction is committed.
type Txn struct {
	readOnly bool
	// Map from job ids to jobs.
	jobsById *immutable.Map[string, *Job]
	// Map from run ids to jobs.
	// Note that a job may have multiple runs, i.e., the mapping is many-to-one.
	jobsByRunId *immutable.Map[uuid.UUID, string]
	// Queued jobs for each queue. Stored in the order in which they should be scheduled.
	jobsByQueue map[string]immutable.SortedSet[*Job]
	// Queued jobs for each queue ordered by remaining time-to-live.
	// TODO: The ordering is wrong. Since we call time.Now() in the compare function.
	queuedJobsByTtl *immutable.SortedSet[*Job]
	// The jobDb from which this transaction was created.
	jobDb *JobDb
	// Set to false when this transaction is either committed or aborted.
	active bool
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
	txn.jobDb.queuedJobsByTtl = txn.queuedJobsByTtl
	txn.active = false
}

// Assert returns an error if the jobDb, or any job stored in the jobDb, is in an invalid state.
// If assertOnlyActiveJobs is true, it also asserts that all jobs in the jobDb are active.
func (txn *Txn) Assert(assertOnlyActiveJobs bool) error {
	it := txn.jobsById.Iterator()
	for {
		jobId, job, ok := it.Next()
		if !ok {
			break
		}
		if job.Id() != jobId {
			return errors.Errorf("jobDb contains a job with misaligned jobId: %s != %s", job.Id(), jobId)
		}
		if err := job.Assert(); err != nil {
			return errors.WithMessage(err, "jobDb is invalid")
		}
		if assertOnlyActiveJobs && job.InTerminalState() {
			return errors.Errorf("jobDb contains an inactive job %s", job)
		}
		if job.Queued() {
			if queue, ok := txn.jobsByQueue[job.queue]; !ok {
				return errors.Errorf("jobDb contains queued job %s but there is no sorted set for this queue", job)
			} else if !queue.Has(job) {
				return errors.Errorf("jobDb contains queued job %s but this job is not in the queue sorted set", job)
			}
		}
		for runId := range job.runsById {
			if otherJobId, ok := txn.jobsByRunId.Get(runId); !ok {
				return errors.Errorf("jobDb contains job %s but there is no mapping from runId %s to this job", job, runId)
			} else if jobId != otherJobId {
				return errors.Errorf("jobDb contains job %s but runId %s does not map to this job", job, runId)
			}
		}
	}
	for queue, queueIt := range txn.jobsByQueue {
		it := queueIt.Iterator()
		for {
			job, ok := it.Next()
			if !ok {
				break
			}
			if job.queue != queue {
				return errors.Errorf("jobDb queue %s contains job %s but this job is in queue %s", queue, job, job.queue)
			} else if other, ok := txn.jobsById.Get(job.id); !ok {
				return errors.Errorf("jobDb queue %s contains job %s but this job is not in the jobDb", queue, job)
			} else if !job.Equal(other) {
				return errors.Errorf("jobDb queue %s contains job %s but this job differs from that stored in the jobDb %s", queue, job, other)
			}
		}
	}
	return nil
}

func (txn *Txn) AssertEqual(otherTxn *Txn) error {
	var result *multierror.Error
	it := txn.jobsById.Iterator()
	for {
		jobId, job, ok := it.Next()
		if !ok {
			break
		}
		otherJob := otherTxn.GetById(jobId)
		if job != nil && otherJob == nil {
			result = multierror.Append(result, errors.Errorf("job %s is not in otherTxn", jobId))
		} else if job == nil && otherTxn != nil {
			result = multierror.Append(result, errors.Errorf("job %s is not in txn", jobId))
		} else if !job.Equal(otherJob) {
			result = multierror.Append(result, errors.Errorf("job %s differs between txn and otherTxn: %s is not equal to %s", jobId, job, otherJob))
		}
	}
	it = otherTxn.jobsById.Iterator()
	for {
		jobId, _, ok := it.Next()
		if !ok {
			break
		}
		job := otherTxn.GetById(jobId)
		if job == nil && otherTxn != nil {
			result = multierror.Append(result, errors.Errorf("job %s is not in txn", jobId))
		}
	}
	if err := result.ErrorOrNil(); err != nil {
		return errors.Wrap(err, "jobDb transactions are not equal")
	}
	return nil
}

func (txn *Txn) Abort() {
	if txn.readOnly || !txn.active {
		return
	}
	txn.active = false
	txn.jobDb.writerMutex.Unlock()
}

// Upsert will insert the given jobs if they don't already exist or update them if they do.
func (txn *Txn) Upsert(jobs []*Job) error {
	if err := txn.checkWritableTransaction(); err != nil {
		return err
	}

	hasJobs := txn.jobsById.Len() > 0

	// First, delete any jobs to be upserted from the set of queued jobs.
	// This to ensure jobs that are no longer queued do not appear in this set.
	// Jobs that are still queued will be re-inserted later.
	if hasJobs {
		for _, job := range jobs {
			existingJob, ok := txn.jobsById.Get(job.id)
			if ok {
				existingQueue, ok := txn.jobsByQueue[existingJob.queue]
				if ok {
					txn.jobsByQueue[existingJob.queue] = existingQueue.Delete(existingJob)
				}

				newQueuedJobsByTtl := txn.queuedJobsByTtl.Delete(existingJob)
				txn.queuedJobsByTtl = &newQueuedJobsByTtl
			}
		}
	}

	// Now need to insert jobs, runs and queuedJobs. This can be done in parallel.
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

	// Queued jobs are additionally stored in an ordered set.
	// To enable iterating over them in the order they should be scheduled.
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

				if job.HasQueueTtlSet() {
					queuedJobsByTtl := txn.queuedJobsByTtl.Add(job)
					txn.queuedJobsByTtl = &queuedJobsByTtl
				}
			}
		}
	}()
	wg.Wait()
	return nil
}

// GetById returns the job with the given Id or nil if no such job exists
// The Job returned by this function *must not* be subsequently modified
func (txn *Txn) GetById(id string) *Job {
	j, _ := txn.jobsById.Get(id)
	return j
}

// GetByRunId returns the job with the given run id or nil if no such job exists
// The Job returned by this function *must not* be subsequently modified
func (txn *Txn) GetByRunId(runId uuid.UUID) *Job {
	jobId, _ := txn.jobsByRunId.Get(runId)
	return txn.GetById(jobId)
}

// HasQueuedJobs returns true if the queue has any jobs in the running state or false otherwise
func (txn *Txn) HasQueuedJobs(queue string) bool {
	queuedJobs, ok := txn.jobsByQueue[queue]
	if !ok {
		return false
	}
	return queuedJobs.Len() > 0
}

// QueuedJobs returns true if the queue has any jobs in the running state or false otherwise
func (txn *Txn) QueuedJobs(queue string) *immutable.SortedSetIterator[*Job] {
	jobQueue, ok := txn.jobsByQueue[queue]
	if ok {
		return jobQueue.Iterator()
	} else {
		return emptyList.Iterator()
	}
}

// QueuedJobsByTtl returns an iterator for jobs ordered by queue ttl time - the closest to expiry first
func (txn *Txn) QueuedJobsByTtl() *immutable.SortedSetIterator[*Job] {
	return txn.queuedJobsByTtl.Iterator()
}

// GetAll returns all jobs in the database.
// The Jobs returned by this function *must not* be subsequently modified
func (txn *Txn) GetAll() []*Job {
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
func (txn *Txn) BatchDelete(jobIds []string) error {
	if err := txn.checkWritableTransaction(); err != nil {
		return err
	}
	for _, id := range jobIds {
		txn.delete(id)
	}
	return nil
}

// delete a job from the txn.
// The caller is responsible for checking that this is a writable txn by calling checkWritableTransaction.
func (txn *Txn) delete(jobId string) {
	job, present := txn.jobsById.Get(jobId)
	if present {
		txn.jobsById = txn.jobsById.Delete(jobId)
		for _, run := range job.runsById {
			txn.jobsByRunId = txn.jobsByRunId.Delete(run.id)
		}
		queue, ok := txn.jobsByQueue[job.queue]
		if ok {
			newQueue := queue.Delete(job)
			txn.jobsByQueue[job.queue] = newQueue
		}

		// We only add these jobs into the collection if it has a queueTtl set, hence only remove if this is set.
		if job.HasQueueTtlSet() {
			newQueuedJobsByExpiry := txn.queuedJobsByTtl.Delete(job)
			txn.queuedJobsByTtl = &newQueuedJobsByExpiry
		}
	}
}

func (txn *Txn) checkWritableTransaction() error {
	if txn.readOnly {
		return errors.New("Cannot write using a read only transaction")
	}
	if !txn.active {
		return errors.New("Cannot write using an inactive transaction")
	}
	return nil
}
