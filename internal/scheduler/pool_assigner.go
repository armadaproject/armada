package scheduler

import (
	"time"

	"github.com/gogo/protobuf/proto"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// PoolAssigner allows jobs to be assigned to a pool
// Note that this is intended only for use with metrics calculation
type PoolAssigner interface {
	Refresh(ctx *armadacontext.Context) error
	AssignPool(j *jobdb.Job) (string, error)
}

type executor struct {
	nodeDb         *nodedb.NodeDb
	minimumJobSize schedulerobjects.ResourceList
}

type DefaultPoolAssigner struct {
	executorTimeout        time.Duration
	priorityClasses        map[string]types.PriorityClass
	priorities             []int32
	indexedResources       []configuration.IndexedResource
	indexedTaints          []string
	indexedNodeLabels      []string
	wellKnownNodeTypes     []configuration.WellKnownNodeType
	poolByExecutorId       map[string]string
	executorsByPool        map[string][]*executor
	executorRepository     database.ExecutorRepository
	schedulingKeyGenerator *schedulerobjects.SchedulingKeyGenerator
	poolCache              *lru.Cache
	clock                  clock.Clock
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
		executorTimeout:        executorTimeout,
		priorityClasses:        schedulingConfig.Preemption.PriorityClasses,
		executorsByPool:        map[string][]*executor{},
		poolByExecutorId:       map[string]string{},
		priorities:             schedulingConfig.Preemption.AllowedPriorities(),
		indexedResources:       schedulingConfig.IndexedResources,
		indexedTaints:          schedulingConfig.IndexedTaints,
		wellKnownNodeTypes:     schedulingConfig.WellKnownNodeTypes,
		indexedNodeLabels:      schedulingConfig.IndexedNodeLabels,
		executorRepository:     executorRepository,
		schedulingKeyGenerator: schedulerobjects.NewSchedulingKeyGenerator(),
		poolCache:              poolCache,
		clock:                  clock.RealClock{},
	}, nil
}

// Refresh updates executor state
func (p *DefaultPoolAssigner) Refresh(ctx *armadacontext.Context) error {
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
	p.schedulingKeyGenerator = schedulerobjects.NewSchedulingKeyGenerator()
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
	var priority int32
	if priorityClass, ok := p.priorityClasses[j.GetPriorityClassName()]; ok {
		priority = priorityClass.Priority
	}
	schedulingKey := p.schedulingKeyGenerator.Key(
		j.GetNodeSelector(),
		j.GetAffinity(),
		j.GetTolerations(),
		j.GetResourceRequirements().Requests,
		priority,
	)
	if cachedPool, ok := p.poolCache.Get(schedulingKey); ok {
		return cachedPool.(string), nil
	}

	req := j.PodRequirements()
	req = p.clearAnnotations(req)

	// Otherwise iterate through each pool and detect the first one the job is potentially schedulable on.
	// TODO: We should use the real scheduler instead since this check may go out of sync with the scheduler.
	for pool, executors := range p.executorsByPool {
		for _, e := range executors {
			requests := req.GetResourceRequirements().Requests
			if ok, _ := constraints.RequestsAreLargeEnough(schedulerobjects.ResourceListFromV1ResourceList(requests), e.minimumJobSize); !ok {
				continue
			}
			nodeDb := e.nodeDb
			txn := nodeDb.Txn(true)
			jctx := &schedulercontext.JobSchedulingContext{
				Created:            time.Now(),
				JobId:              j.GetId(),
				Job:                j,
				PodRequirements:    j.GetPodRequirements(p.priorityClasses),
				GangMinCardinality: 1,
			}
			node, err := nodeDb.SelectNodeForJobWithTxn(txn, jctx)
			txn.Abort()
			if err != nil {
				return "", errors.WithMessagef(err, "error selecting node for job %s", j.Id())
			}
			if node != nil {
				p.poolCache.Add(schedulingKey, pool)
				return pool, nil
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
		p.wellKnownNodeTypes,
	)
	if err != nil {
		return nil, err
	}
	txn := nodeDb.Txn(true)
	defer txn.Abort()
	for _, node := range nodes {
		if err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node); err != nil {
			return nil, err
		}
	}
	txn.Commit()
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
