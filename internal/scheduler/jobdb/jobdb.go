package jobdb

import (
	"fmt"
	"sync"

	"github.com/benbjohnson/immutable"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/constants"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/scheduler/adapters"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/pkg/bidstore"
)

type JobSortOrder int

const (
	PriceOrder JobSortOrder = iota
	FairShareOrder
)

type JobIterator interface {
	Next() (*Job, bool)
	Done() bool
}

type PoolIterator struct {
	it   JobIterator
	pool string
}

func (p PoolIterator) Next() (*Job, bool) {
	for !p.it.Done() {
		job, exists := p.it.Next()
		if !exists {
			return nil, false
		}
		if slices.Contains(job.Pools(), p.pool) {
			return job, true
		}
	}
	return nil, false
}

func (p PoolIterator) Done() bool {
	return p.it.Done()
}

var emptyList = immutable.NewSortedSet[*Job](JobPriorityComparer{})

type gangKey struct {
	queue  string
	gangId string
}

type JobDb struct {
	jobsById           *immutable.Map[string, *Job]
	jobsByRunId        *immutable.Map[string, string]
	jobsByGangKey      map[gangKey]immutable.Set[string]
	jobsByQueue        map[string]immutable.SortedSet[*Job]
	jobsByPoolAndQueue map[string]map[string]immutable.SortedSet[*Job]
	unvalidatedJobs    *immutable.Set[*Job]
	// Configured priority classes.
	priorityClasses map[string]types.PriorityClass
	// Priority class assigned to jobs with a priorityClassName not in jobDb.priorityClasses.
	defaultPriorityClass   types.PriorityClass
	schedulingKeyGenerator *internaltypes.SchedulingKeyGenerator
	// We intern strings to save memory.
	stringInterner *stringinterner.StringInterner
	// Mutexes protecting the jobDb.
	copyMutex   sync.Mutex
	writerMutex sync.Mutex
	// Clock used when assigning timestamps to created job runs.
	// Set here so that it can be mocked.
	clock clock.PassiveClock
	// Used for generating job run ids.
	uuidProvider IDProvider
	// Used to make efficient ResourceList types.
	resourceListFactory *internaltypes.ResourceListFactory
}

// IDProvider is an interface used to mock run id  generation for tests.
type IDProvider interface {
	New() string
}

// RealUUIDProvider generates an id using a UUID
type RealUUIDProvider struct{}

func (_ RealUUIDProvider) New() string {
	return uuid.New().String()
}

func NewJobDb(priorityClasses map[string]types.PriorityClass,
	defaultPriorityClassName string,
	stringInterner *stringinterner.StringInterner,
	resourceListFactory *internaltypes.ResourceListFactory,
) *JobDb {
	return NewJobDbWithSchedulingKeyGenerator(
		priorityClasses,
		defaultPriorityClassName,
		internaltypes.NewSchedulingKeyGenerator(),
		stringInterner,
		resourceListFactory,
	)
}

func NewJobDbWithSchedulingKeyGenerator(
	priorityClasses map[string]types.PriorityClass,
	defaultPriorityClassName string,
	skg *internaltypes.SchedulingKeyGenerator,
	stringInterner *stringinterner.StringInterner,
	resourceListFactory *internaltypes.ResourceListFactory,
) *JobDb {
	defaultPriorityClass, ok := priorityClasses[defaultPriorityClassName]
	if !ok {
		// TODO(albin): Return an error instead.
		panic(fmt.Sprintf("unknown default priority class %s", defaultPriorityClassName))
	}
	unvalidatedJobs := immutable.NewSet[*Job](JobHasher{})
	return &JobDb{
		jobsById:               immutable.NewMap[string, *Job](nil),
		jobsByRunId:            immutable.NewMap[string, string](nil),
		jobsByGangKey:          map[gangKey]immutable.Set[string]{},
		jobsByQueue:            map[string]immutable.SortedSet[*Job]{},
		jobsByPoolAndQueue:     map[string]map[string]immutable.SortedSet[*Job]{},
		unvalidatedJobs:        &unvalidatedJobs,
		priorityClasses:        priorityClasses,
		defaultPriorityClass:   defaultPriorityClass,
		schedulingKeyGenerator: skg,
		stringInterner:         stringInterner,
		clock:                  clock.RealClock{},
		uuidProvider:           RealUUIDProvider{},
		resourceListFactory:    resourceListFactory,
	}
}

func (jobDb *JobDb) SetClock(clock clock.PassiveClock) {
	jobDb.clock = clock
}

func (jobDb *JobDb) SetUUIDProvider(uuidProvider IDProvider) {
	jobDb.uuidProvider = uuidProvider
}

// Clone returns a copy of the jobDb.
func (jobDb *JobDb) Clone() *JobDb {
	return &JobDb{
		jobsById:               jobDb.jobsById,
		jobsByRunId:            jobDb.jobsByRunId,
		jobsByGangKey:          maps.Clone(jobDb.jobsByGangKey),
		jobsByQueue:            maps.Clone(jobDb.jobsByQueue),
		jobsByPoolAndQueue:     maps.Clone(jobDb.jobsByPoolAndQueue),
		unvalidatedJobs:        jobDb.unvalidatedJobs,
		priorityClasses:        jobDb.priorityClasses,
		defaultPriorityClass:   jobDb.defaultPriorityClass,
		schedulingKeyGenerator: jobDb.schedulingKeyGenerator,
		stringInterner:         jobDb.stringInterner,
		resourceListFactory:    jobDb.resourceListFactory,
	}
}

// NewJob creates a new scheduler job.
// The new job is not automatically inserted into the jobDb; call jobDb.Upsert to upsert it.
func (jobDb *JobDb) NewJob(
	jobId string,
	jobSet string,
	queue string,
	priority uint32,
	schedulingInfo *internaltypes.JobSchedulingInfo,
	queued bool,
	queuedVersion int32,
	cancelRequested bool,
	cancelByJobSetRequested bool,
	cancelled bool,
	created int64,
	validated bool,
	pools []string,
	priceBand int32,
) (*Job, error) {
	schedulingInfo = jobDb.internJobSchedulingInfoStrings(schedulingInfo)
	priorityClass, ok := jobDb.priorityClasses[schedulingInfo.PriorityClass]
	if !ok {
		priorityClass = jobDb.defaultPriorityClass
	}

	rr := jobDb.getResourceRequirements(schedulingInfo)

	_, ok = bidstore.PriceBand_name[priceBand]
	pb := bidstore.PriceBand_PRICE_BAND_UNSPECIFIED
	if ok {
		pb = bidstore.PriceBand(priceBand)
	}

	gangInfo, err := GangInfoFromMinimalJob(schedulingInfo)
	if err != nil {
		log.Errorf("failed creating gang info for job %s", jobId)
		// TODO should we error here or continue on interpreting the job as not a gang job?
		return nil, err
	}

	reservations := map[string]bool{}
	if schedulingInfo.PodRequirements != nil {
		for _, toleration := range schedulingInfo.PodRequirements.Tolerations {
			if toleration.Key == constants.ReservationTaintKey {
				reservations[toleration.Value] = true
			}
		}
	}

	job := &Job{
		jobDb:                          jobDb,
		id:                             jobId,
		queue:                          jobDb.stringInterner.Intern(queue),
		jobSet:                         jobDb.stringInterner.Intern(jobSet),
		priority:                       priority,
		queued:                         queued,
		queuedVersion:                  queuedVersion,
		requestedPriority:              priority,
		submittedTime:                  created,
		jobSchedulingInfo:              schedulingInfo,
		allResourceRequirements:        rr,
		kubernetesResourceRequirements: rr.OfType(internaltypes.Kubernetes),
		priorityClass:                  priorityClass,
		cancelRequested:                cancelRequested,
		cancelByJobSetRequested:        cancelByJobSetRequested,
		cancelled:                      cancelled,
		validated:                      validated,
		runsById:                       map[string]*JobRun{},
		pools:                          jobDb.internPools(pools),
		priceBand:                      pb,
		gangInfo:                       *gangInfo,
		reservations:                   reservations,
	}
	job.ensureJobSchedulingInfoFieldsInitialised()
	job.schedulingKey = SchedulingKeyFromJob(jobDb.schedulingKeyGenerator, job)
	return job, nil
}

func (jobDb *JobDb) getResourceRequirements(schedulingInfo *internaltypes.JobSchedulingInfo) internaltypes.ResourceList {
	return jobDb.resourceListFactory.FromJobResourceListIgnoreUnknown(safeGetRequirements(schedulingInfo))
}

func safeGetRequirements(schedulingInfo *internaltypes.JobSchedulingInfo) map[string]resource.Quantity {
	pr := schedulingInfo.PodRequirements
	if pr == nil {
		return map[string]resource.Quantity{}
	}

	req := pr.ResourceRequirements.Requests
	if req == nil {
		return map[string]resource.Quantity{}
	}

	return adapters.K8sResourceListToMap(req)
}

func (jobDb *JobDb) internJobSchedulingInfoStrings(info *internaltypes.JobSchedulingInfo) *internaltypes.JobSchedulingInfo {
	info.PriorityClass = jobDb.stringInterner.Intern(info.PriorityClass)
	pr := info.PodRequirements
	newAnnotations := make(map[string]string, len(pr.Annotations))
	for k, v := range pr.Annotations {
		newAnnotations[jobDb.stringInterner.Intern(k)] = jobDb.stringInterner.Intern(v)
	}
	pr.Annotations = newAnnotations

	newNodeSelector := make(map[string]string, len(pr.NodeSelector))
	for k, v := range pr.NodeSelector {
		newNodeSelector[jobDb.stringInterner.Intern(k)] = jobDb.stringInterner.Intern(v)
	}
	pr.NodeSelector = newNodeSelector

	for idx, toleration := range pr.Tolerations {
		pr.Tolerations[idx] = v1.Toleration{
			Key:               jobDb.stringInterner.Intern(toleration.Key),
			Operator:          v1.TolerationOperator(jobDb.stringInterner.Intern(string(toleration.Operator))),
			Value:             jobDb.stringInterner.Intern(toleration.Value),
			Effect:            v1.TaintEffect(jobDb.stringInterner.Intern(string(toleration.Effect))),
			TolerationSeconds: toleration.TolerationSeconds,
		}
	}

	for idx, claim := range pr.ResourceRequirements.Claims {
		pr.ResourceRequirements.Claims[idx].Name = jobDb.stringInterner.Intern(claim.Name)
	}

	for key, limit := range pr.ResourceRequirements.Limits {
		limit.Format = resource.Format(jobDb.stringInterner.Intern(string(limit.Format)))
		pr.ResourceRequirements.Limits[v1.ResourceName(jobDb.stringInterner.Intern(string(key)))] = limit
	}

	for key, request := range pr.ResourceRequirements.Requests {
		request.Format = resource.Format(jobDb.stringInterner.Intern(string(request.Format)))
		pr.ResourceRequirements.Requests[v1.ResourceName(jobDb.stringInterner.Intern(string(key)))] = request
	}

	return info
}

func (jobDb *JobDb) internPools(pools []string) []string {
	newPools := make([]string, len(pools))
	for idx, pool := range pools {
		newPools[idx] = jobDb.stringInterner.Intern(pool)
	}
	return newPools
}

// ReadTxn returns a read-only transaction.
// Multiple read-only transactions can access the db concurrently
func (jobDb *JobDb) ReadTxn() *Txn {
	jobDb.copyMutex.Lock()
	defer jobDb.copyMutex.Unlock()
	return &Txn{
		readOnly:           true,
		jobsById:           jobDb.jobsById,
		jobsByRunId:        jobDb.jobsByRunId,
		jobsByGangKey:      jobDb.jobsByGangKey,
		jobsByQueue:        jobDb.jobsByQueue,
		jobsByPoolAndQueue: jobDb.jobsByPoolAndQueue,
		unvalidatedJobs:    jobDb.unvalidatedJobs,
		active:             true,
		jobDb:              jobDb,
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
		readOnly:           false,
		jobsById:           jobDb.jobsById,
		jobsByRunId:        jobDb.jobsByRunId,
		jobsByGangKey:      maps.Clone(jobDb.jobsByGangKey),
		jobsByQueue:        maps.Clone(jobDb.jobsByQueue),
		jobsByPoolAndQueue: maps.Clone(jobDb.jobsByPoolAndQueue),
		unvalidatedJobs:    jobDb.unvalidatedJobs,
		active:             true,
		jobDb:              jobDb,
	}
}

func (jobDb *JobDb) CumulativeInternedStringsCount() uint64 {
	return jobDb.stringInterner.CumulativeInsertCount()
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
	jobsByRunId *immutable.Map[string, string]
	// Map from gang key (queue + gangId) to jobs ids
	// This represents all known jobs in with the same gang key
	jobsByGangKey map[gangKey]immutable.Set[string]
	// Queued jobs for each queue. Stored in the order in which they should be scheduled.
	jobsByQueue map[string]immutable.SortedSet[*Job]
	// Queued jobs for each queue and pool.
	// Stored as a set and needs sorting to determine the order they should be scheduled in.
	jobsByPoolAndQueue map[string]map[string]immutable.SortedSet[*Job]
	// Jobs that require submit checking
	unvalidatedJobs *immutable.Set[*Job]
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
	txn.jobDb.jobsByGangKey = txn.jobsByGangKey
	txn.jobDb.jobsByQueue = txn.jobsByQueue
	txn.jobDb.jobsByPoolAndQueue = txn.jobsByPoolAndQueue
	txn.jobDb.unvalidatedJobs = txn.unvalidatedJobs

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

	// First, delete any jobs to be upserted from the sets of queued and unvalidated jobs
	// We will replace these jobs later if they are still queued
	if hasJobs {
		for _, job := range jobs {
			existingJob, ok := txn.jobsById.Get(job.id)
			if ok {
				existingQueue, ok := txn.jobsByQueue[existingJob.queue]
				if ok {
					txn.jobsByQueue[existingJob.queue] = existingQueue.Delete(existingJob)
				}

				for _, pool := range job.Pools() {
					_, present := txn.jobsByPoolAndQueue[pool]
					if !present {
						continue
					}
					existingJobs, present := txn.jobsByPoolAndQueue[pool][job.queue]
					if !present {
						continue
					}
					txn.jobsByPoolAndQueue[pool][job.queue] = existingJobs.Delete(existingJob)
				}

				if !existingJob.Validated() {
					newUnvalidatedJobs := txn.unvalidatedJobs.Delete(existingJob)
					txn.unvalidatedJobs = &newUnvalidatedJobs
				}
			}
		}
	}

	// Now need to insert jobs, runs and queuedJobs. This can be done in parallel.
	wg := sync.WaitGroup{}
	wg.Add(5)

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
			jobsByRunId := immutable.NewMapBuilder[string, string](nil)
			for _, job := range jobs {
				for _, run := range job.runsById {
					jobsByRunId.Set(run.id, job.id)
				}
			}
			txn.jobsByRunId = jobsByRunId.Map()
		}
	}()

	// gangs
	go func() {
		defer wg.Done()
		if hasJobs {
			for _, job := range jobs {
				if job.IsInGang() {
					key := gangKey{queue: job.Queue(), gangId: job.GetGangInfo().Id()}

					if _, present := txn.jobsByGangKey[key]; !present {
						txn.jobsByGangKey[key] = immutable.NewSet[string](JobIdHasher{})
					}
					txn.jobsByGangKey[key] = txn.jobsByGangKey[key].Add(job.Id())
				}
			}
		} else {
			jobsByGangKey := map[gangKey]map[string]bool{}
			for _, job := range jobs {
				if job.IsInGang() {
					key := gangKey{queue: job.Queue(), gangId: job.GetGangInfo().Id()}

					if _, present := jobsByGangKey[key]; !present {
						jobsByGangKey[key] = map[string]bool{}
					}
					jobsByGangKey[key][job.Id()] = true
				}
			}

			for key, jobsInGang := range jobsByGangKey {
				txn.jobsByGangKey[key] = immutable.NewSet[string](JobIdHasher{}, maps.Keys(jobsInGang)...)
			}
		}
	}()

	// Queued jobs are additionally stored in an ordered set.
	// To enable iterating over them in the order they should be scheduled.
	go func() {
		defer wg.Done()
		if hasJobs {
			for _, job := range jobs {
				if job.Queued() {
					newQueue, ok := txn.jobsByQueue[job.queue]
					if !ok {
						newQueue = emptyList
					}
					txn.jobsByQueue[job.queue] = newQueue.Add(job)

					for _, pool := range job.Pools() {
						_, present := txn.jobsByPoolAndQueue[pool]
						if !present {
							queues := map[string]immutable.SortedSet[*Job]{}
							txn.jobsByPoolAndQueue[pool] = queues
						}
						_, present = txn.jobsByPoolAndQueue[pool][job.queue]
						if !present {
							jobs := immutable.NewSortedSet[*Job](MarketJobPriorityComparer{Pool: pool})
							txn.jobsByPoolAndQueue[pool][job.queue] = jobs
						}
						txn.jobsByPoolAndQueue[pool][job.queue] = txn.jobsByPoolAndQueue[pool][job.queue].Add(job)
					}
				}
			}
		} else {
			jobsByQueue := map[string]map[*Job]bool{}
			jobsByPoolAndQueue := map[string]map[string]map[*Job]bool{}

			for _, job := range jobs {
				if job.Queued() {
					if _, ok := jobsByQueue[job.queue]; !ok {
						jobsByQueue[job.queue] = map[*Job]bool{}
					}
					jobsByQueue[job.queue][job] = true

					for _, pool := range job.Pools() {
						if _, present := jobsByPoolAndQueue[pool]; !present {
							jobsByPoolAndQueue[pool] = map[string]map[*Job]bool{}
						}
						if _, present := jobsByPoolAndQueue[pool][job.queue]; !present {
							jobsByPoolAndQueue[pool][job.queue] = map[*Job]bool{}
						}
						jobsByPoolAndQueue[pool][job.queue][job] = true
					}
				}
			}

			for queue, jobsForQueue := range jobsByQueue {
				txn.jobsByQueue[queue] = immutable.NewSortedSet[*Job](JobPriorityComparer{}, maps.Keys(jobsForQueue)...)
			}

			for pool, jobsForPool := range jobsByPoolAndQueue {
				if _, ok := txn.jobsByPoolAndQueue[pool]; !ok {
					txn.jobsByPoolAndQueue[pool] = map[string]immutable.SortedSet[*Job]{}
				}
				for queue, jobsForQueueInPool := range jobsForPool {
					if _, ok := txn.jobsByPoolAndQueue[pool][queue]; !ok {
						txn.jobsByPoolAndQueue[pool][queue] = immutable.NewSortedSet[*Job](MarketJobPriorityComparer{Pool: pool}, maps.Keys(jobsForQueueInPool)...)
					}
				}
			}
		}
	}()

	// Unvalidated jobs
	go func() {
		defer wg.Done()
		if hasJobs {
			for _, job := range jobs {
				if !job.Validated() {
					unvalidatedJobs := txn.unvalidatedJobs.Add(job)
					txn.unvalidatedJobs = &unvalidatedJobs
				}
			}
		} else {
			unvalidatedJobs := map[*Job]bool{}

			for _, job := range jobs {
				if !job.Validated() {
					unvalidatedJobs[job] = true
				}
			}

			unvalidatedJobsImmutable := immutable.NewSet[*Job](JobHasher{}, maps.Keys(unvalidatedJobs)...)
			txn.unvalidatedJobs = &unvalidatedJobsImmutable
		}
	}()

	wg.Wait()
	return nil
}

// NewJob creates a new scheduler job.
// The new job is not automatically inserted into the jobDb; call jobDb.Upsert to upsert it.
func (txn *Txn) NewJob(
	jobId string,
	jobSet string,
	queue string,
	priority uint32,
	schedulingInfo *internaltypes.JobSchedulingInfo,
	queued bool,
	queuedVersion int32,
	cancelRequested bool,
	cancelByJobSetRequested bool,
	cancelled bool,
	created int64,
	validated bool,
	pools []string,
	priceBand int32,
) (*Job, error) {
	return txn.jobDb.NewJob(jobId,
		jobSet,
		queue,
		priority,
		schedulingInfo,
		queued,
		queuedVersion,
		cancelRequested,
		cancelByJobSetRequested,
		cancelled,
		created,
		validated,
		pools,
		priceBand,
	)
}

// GetById returns the job with the given Id or nil if no such job exists
// The Job returned by this function *must not* be subsequently modified
func (txn *Txn) GetById(id string) *Job {
	j, _ := txn.jobsById.Get(id)
	return j
}

func (txn *Txn) GetGangJobsIdsByGangId(queue string, gangId string) []string {
	jobIdsSet, present := txn.jobsByGangKey[gangKey{queue: queue, gangId: gangId}]
	if !present {
		return []string{}
	}
	return jobIdsSet.Items()
}

func (txn *Txn) GetGangJobsByGangId(queue string, gangId string) ([]*Job, error) {
	jobIdsSet, present := txn.jobsByGangKey[gangKey{queue: queue, gangId: gangId}]
	if !present {
		return []*Job{}, nil
	}
	jobs := make([]*Job, 0, jobIdsSet.Len())
	for _, jobId := range jobIdsSet.Items() {
		j, exists := txn.jobsById.Get(jobId)
		if exists {
			jobs = append(jobs, j)
		} else {
			return nil, fmt.Errorf("could not find job with id %s when retrieving jobs by gang id %s - %s", jobId, queue, gangId)
		}
	}
	return jobs, nil
}

// GetByRunId returns the job with the given run id or nil if no such job exists
// The Job returned by this function *must not* be subsequently modified
func (txn *Txn) GetByRunId(runId string) *Job {
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

// QueuedJobs returns an iterator for the jobs queued for that provided queue + pool
func (txn *Txn) QueuedJobs(queue string, pool string, sortOrder JobSortOrder) JobIterator {
	if sortOrder == PriceOrder {
		if _, ok := txn.jobsByPoolAndQueue[pool]; !ok {
			return emptyList.Iterator()
		}
		_, ok := txn.jobsByPoolAndQueue[pool][queue]
		if !ok {
			return emptyList.Iterator()
		}
		return txn.jobsByPoolAndQueue[pool][queue].Iterator()
	} else {
		jobQueue, ok := txn.jobsByQueue[queue]
		if !ok {
			return emptyList.Iterator()
		}
		return PoolIterator{it: jobQueue.Iterator(), pool: pool}
	}
}

// UnvalidatedJobs returns an iterator for jobs that have not yet been validated
func (txn *Txn) UnvalidatedJobs() *immutable.SetIterator[*Job] {
	return txn.unvalidatedJobs.Iterator()
}

// GetAll returns all jobs in the database.
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
		for _, pool := range job.Pools() {
			_, present := txn.jobsByPoolAndQueue[pool]
			if !present {
				continue
			}
			existingJobs, present := txn.jobsByPoolAndQueue[pool][job.queue]
			if !present {
				continue
			}
			txn.jobsByPoolAndQueue[pool][job.queue] = existingJobs.Delete(job)
		}
		if job.IsInGang() {
			key := gangKey{queue: job.queue, gangId: job.GetGangInfo().Id()}
			gangJobIds, ok := txn.jobsByGangKey[key]
			if ok {
				newGangJobIds := gangJobIds.Delete(job.Id())
				if newGangJobIds.Len() > 0 {
					txn.jobsByGangKey[key] = newGangJobIds
				} else {
					delete(txn.jobsByGangKey, key)
				}
			}
		}
		newUnvalidatedJobs := txn.unvalidatedJobs.Delete(job)
		txn.unvalidatedJobs = &newUnvalidatedJobs
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
