package scheduler

import (
	"context"
	"time"

	"github.com/gogo/protobuf/proto"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// PoolAssigner allows jobs to be assigned to a pool
// Note that this is intended only for use with metrics calculation
type PoolAssigner interface {
	Refresh(ctx context.Context) error
	AssignPool(j *jobdb.Job) (string, error)
}

type executor struct {
	nodeDb         *nodedb.NodeDb
	minimumJobSize schedulerobjects.ResourceList
}

type DefaultPoolAssigner struct {
	executorTimeout    time.Duration
	priorityClasses    map[string]configuration.PriorityClass
	priorities         []int32
	indexedResources   []string
	indexedTaints      []string
	indexedNodeLabels  []string
	poolByExecutorId   map[string]string
	executorsByPool    map[string][]*executor
	executorRepository database.ExecutorRepository
	poolCache          *lru.Cache
	clock              clock.Clock
}

func NewPoolAssigner(executorTimeout time.Duration,
	schedulingConfig configuration.SchedulingConfig,
	executorRepository database.ExecutorRepository,
) (*DefaultPoolAssigner, error) {
	poolCache, err := lru.New(maxJobSchedulingResults)
	if err != nil {
		return nil, errors.Wrap(err, "error  creating PoolAssigner pool cache")
	}
	return &DefaultPoolAssigner{
		executorTimeout:    executorTimeout,
		priorityClasses:    schedulingConfig.Preemption.PriorityClasses,
		executorsByPool:    map[string][]*executor{},
		poolByExecutorId:   map[string]string{},
		priorities:         schedulingConfig.Preemption.AllowedPriorities(),
		indexedResources:   schedulingConfig.IndexedResources,
		indexedTaints:      schedulingConfig.IndexedTaints,
		indexedNodeLabels:  schedulingConfig.IndexedNodeLabels,
		executorRepository: executorRepository,
		poolCache:          poolCache,
		clock:              clock.RealClock{},
	}, nil
}

// Refresh updates executor state
func (p *DefaultPoolAssigner) Refresh(ctx context.Context) error {
	executors, err := p.executorRepository.GetExecutors(ctx)
	executorsByPool := map[string][]*executor{}
	poolByExecutorId := map[string]string{}
	if err != nil {
		return err
	}
	for _, e := range executors {
		if p.clock.Since(e.LastUpdateTime) < p.executorTimeout {
			poolByExecutorId[e.Id] = e.Pool
			nodeDb, err := p.constructNodeDb(e.Nodes)
			if err != nil {
				return errors.WithMessagef(err, "could not construct node db for executor %s", e.Id)
			}
			executorsByPool[e.Pool] = append(executorsByPool[e.Pool], &executor{
				nodeDb:         nodeDb,
				minimumJobSize: e.MinimumJobSize,
			})
		}
	}
	p.executorsByPool = executorsByPool
	p.poolByExecutorId = poolByExecutorId
	p.poolCache.Purge()
	return nil
}

// AssignPool returns the pool associated with the job or the empty string if no pool is valid
func (p *DefaultPoolAssigner) AssignPool(j *jobdb.Job) (string, error) {
	// If Job is running then use the pool associated with the executor it was assigned to
	if !j.Queued() && j.HasRuns() {
		return p.poolByExecutorId[j.LatestRun().Executor()], nil
	}

	// See if we have this set of reqs cached.
	if schedulingKey, ok := j.JobSchedulingInfo().SchedulingKey(); ok {
		if cachedPool, ok := p.poolCache.Get(schedulingKey); ok {
			return cachedPool.(string), nil
		}
	}

	req := PodRequirementFromJobSchedulingInfo(j.JobSchedulingInfo())
	req = p.clearAnnotations(req)

	// Otherwise iterate through each pool and detect the first one the job is potentially schedulable on
	for pool, executors := range p.executorsByPool {
		for _, e := range executors {
			minReqsMet, _ := requestIsLargeEnough(schedulerobjects.ResourceListFromV1ResourceList(
				req.GetResourceRequirements().Requests,
			), e.minimumJobSize)
			if minReqsMet {
				nodeDb := e.nodeDb
				txn := nodeDb.Txn(true)
				report, err := nodeDb.SelectNodeForPodWithTxn(txn, req)
				txn.Abort()
				if err != nil {
					return "", errors.WithMessagef(err, "error selecting node for job %s", j.Id())
				}
				if report.Node != nil {
					if schedulingKey, ok := j.JobSchedulingInfo().SchedulingKey(); ok {
						p.poolCache.Add(schedulingKey, pool)
					}
					return pool, nil
				}
			}
		}
	}
	return "", nil
}

func (p *DefaultPoolAssigner) constructNodeDb(nodes []*schedulerobjects.Node) (*nodedb.NodeDb, error) {
	// Nodes to be considered by the scheduler.
	nodeDb, err := nodedb.NewNodeDb(
		p.priorityClasses,
		0,
		p.indexedResources,
		p.indexedTaints,
		p.indexedNodeLabels,
	)
	if err != nil {
		return nil, err
	}
	err = nodeDb.UpsertMany(nodes)
	if err != nil {
		return nil, err
	}
	err = nodeDb.ClearAllocated()
	if err != nil {
		return nil, err
	}
	return nodeDb, nil
}

// clearAnnotations
func (p *DefaultPoolAssigner) clearAnnotations(reqs *schedulerobjects.PodRequirements) *schedulerobjects.PodRequirements {
	reqsCopy := proto.Clone(reqs).(*schedulerobjects.PodRequirements)
	for key := range reqsCopy.GetAnnotations() {
		reqsCopy.Annotations[key] = "poolassigner"
	}
	return reqsCopy
}
