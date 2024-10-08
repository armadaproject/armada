package scheduler

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"golang.org/x/exp/maps"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/logging"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type schedulingResult struct {
	isSchedulable bool
	pools         []string
	reason        string
}

type executor struct {
	id     string
	nodeDb *nodedb.NodeDb
}

type schedulerState struct {
	executorsByPoolAndId      map[string]map[string]*executor
	jobSchedulingResultsCache *lru.Cache
}

type SubmitScheduleChecker interface {
	Check(ctx *armadacontext.Context, jobs []*jobdb.Job) (map[string]schedulingResult, error)
}

type SubmitChecker struct {
	schedulingConfig    configuration.SchedulingConfig
	executorRepository  database.ExecutorRepository
	resourceListFactory *internaltypes.ResourceListFactory
	state               atomic.Pointer[schedulerState]
	clock               clock.Clock // can  be  overridden for testing
}

func NewSubmitChecker(
	schedulingConfig configuration.SchedulingConfig,
	executorRepository database.ExecutorRepository,
	resourceListFactory *internaltypes.ResourceListFactory,
) *SubmitChecker {
	return &SubmitChecker{
		schedulingConfig:    schedulingConfig,
		executorRepository:  executorRepository,
		resourceListFactory: resourceListFactory,
		clock:               clock.RealClock{},
	}
}

func (srv *SubmitChecker) Run(ctx *armadacontext.Context) error {
	ctx.Infof("Will refresh executor state every %s", srv.schedulingConfig.ExecutorUpdateFrequency)
	srv.updateExecutors(ctx)
	ticker := time.NewTicker(srv.schedulingConfig.ExecutorUpdateFrequency)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			srv.updateExecutors(ctx)
		}
	}
}

func (srv *SubmitChecker) updateExecutors(ctx *armadacontext.Context) {
	executors, err := srv.executorRepository.GetExecutors(ctx)
	if err != nil {
		logging.
			WithStacktrace(ctx, err).
			Error("Error fetching executors")
		return
	}
	ctx.Infof("Retrieved %d executors", len(executors))
	jobSchedulingResultsCache, err := lru.New(10000)
	if err != nil {
		// This should never happen as lru.New only returns an error if it is initialised with a negative size
		panic(err)
	}

	executorsByPoolAndId := map[string]map[string]*executor{}
	for _, ex := range executors {
		nodes := ex.GetNodes()
		nodesByPool := armadaslices.GroupByFunc(nodes, func(n *schedulerobjects.Node) string {
			return n.GetPool()
		})
		for pool, nodes := range nodesByPool {
			nodeDb, err := srv.constructNodeDb(nodes)

			if _, present := executorsByPoolAndId[pool]; !present {
				executorsByPoolAndId[pool] = map[string]*executor{}
			}

			if err == nil {
				executorsByPoolAndId[pool][ex.Id] = &executor{
					id:     ex.Id,
					nodeDb: nodeDb,
				}
			} else {
				logging.
					WithStacktrace(ctx, err).
					Warnf("Error constructing nodedb for executor: %s", ex.Id)
			}

		}
	}
	srv.state.Store(&schedulerState{
		executorsByPoolAndId:      executorsByPoolAndId,
		jobSchedulingResultsCache: jobSchedulingResultsCache,
	})
}

func (srv *SubmitChecker) Check(ctx *armadacontext.Context, jobs []*jobdb.Job) (map[string]schedulingResult, error) {
	start := time.Now()
	state := srv.state.Load()
	if state == nil {
		return nil, fmt.Errorf("executor state not loaded")
	}

	jobContexts := context.JobSchedulingContextsFromJobs(jobs)
	results := make(map[string]schedulingResult, len(jobs))

	// First, check if all jobs can be scheduled individually.
	for _, jctx := range jobContexts {
		results[jctx.JobId] = srv.getIndividualSchedulingResult(jctx, state)
	}

	// Then, check if all gangs can be scheduled.
	for gangId, jctxs := range armadaslices.GroupByFunc(
		jobContexts,
		func(jctx *context.JobSchedulingContext) string {
			return jctx.GangInfo.Id
		},
	) {
		if gangId == "" {
			continue
		}
		gctx := context.NewGangSchedulingContext(jctxs)
		if result := srv.getSchedulingResult(gctx, state); !result.isSchedulable {
			for _, jctx := range gctx.JobSchedulingContexts {
				results[jctx.JobId] = result
			}
		}
	}
	ctx.Infof("Checked %d jobs in %s", len(jobs), time.Since(start))
	return results, nil
}

func (srv *SubmitChecker) getIndividualSchedulingResult(jctx *context.JobSchedulingContext, state *schedulerState) schedulingResult {
	schedulingKey := jctx.Job.SchedulingKey()

	if obj, ok := state.jobSchedulingResultsCache.Get(schedulingKey); ok {
		return obj.(schedulingResult)
	}

	gangInfo := jctx.GangInfo
	// Mark this job context as "not in a gang" for the individual scheduling check.
	jctx.GangInfo = context.EmptyGangInfo(jctx.Job)
	defer func() {
		jctx.GangInfo = gangInfo
	}()

	gctx := context.NewGangSchedulingContext([]*context.JobSchedulingContext{jctx})
	result := srv.getSchedulingResult(gctx, state)

	state.jobSchedulingResultsCache.Add(schedulingKey, result)

	return result
}

// Check if a set of jobs can be scheduled onto some cluster.
// TODO: there are a number of things this won't catch:
//   - Node Uniformity Label (although it will work if this is per cluster)
//   - Gang jobs that will use more than the allowed capacity limit
func (srv *SubmitChecker) getSchedulingResult(gctx *context.GangSchedulingContext, state *schedulerState) schedulingResult {
	sucessfulPools := map[string]bool{}
	var sb strings.Builder

poolStart:
	for _, pool := range srv.schedulingConfig.Pools {
		if sucessfulPools[pool.Name] {
			continue
		}

		for _, awayPool := range pool.AwayPools {
			if sucessfulPools[awayPool] {
				continue poolStart
			}
		}

		executors := maps.Values(state.executorsByPoolAndId[pool.Name])
		for _, awayPool := range pool.AwayPools {
			executors = append(executors, maps.Values(state.executorsByPoolAndId[awayPool])...)
		}

		for _, ex := range executors {
			txn := ex.nodeDb.Txn(true)
			ok, err := ex.nodeDb.ScheduleManyWithTxn(txn, gctx)
			txn.Abort()

			sb.WriteString(ex.id)
			if err != nil {
				sb.WriteString(err.Error())
				sb.WriteString("\n")
				continue
			}

			if ok {
				if !gctx.JobSchedulingContexts[0].PodSchedulingContext.ScheduledAway || len(pool.AwayPools) > 0 {
					sucessfulPools[pool.Name] = true
				}
				continue
			}

			numSuccessfullyScheduled := 0
			for _, jctx := range gctx.JobSchedulingContexts {
				if jctx.PodSchedulingContext.IsSuccessful() {
					numSuccessfullyScheduled++
				}
			}

			if len(gctx.JobSchedulingContexts) == 1 {
				sb.WriteString(":\n")
				pctx := gctx.JobSchedulingContexts[0].PodSchedulingContext
				if pctx == nil {
					continue
				}
				sb.WriteString(pctx.String())
				sb.WriteString("\n")
				sb.WriteString("---")
				sb.WriteString("\n")
			} else {
				sb.WriteString(
					fmt.Sprintf(
						": %d out of %d pods schedulable (minCardinality %d)\n",
						numSuccessfullyScheduled, len(gctx.JobSchedulingContexts), gctx.GangInfo.Cardinality,
					),
				)
			}
		}
	}
	if len(sucessfulPools) > 0 {
		return schedulingResult{isSchedulable: true, pools: maps.Keys(sucessfulPools)}
	}
	return schedulingResult{isSchedulable: false, reason: sb.String()}
}

func (srv *SubmitChecker) constructNodeDb(nodes []*schedulerobjects.Node) (*nodedb.NodeDb, error) {
	nodeFactory := internaltypes.NewNodeFactory(srv.schedulingConfig.IndexedTaints,
		srv.schedulingConfig.IndexedNodeLabels,
		srv.resourceListFactory)

	nodeDb, err := nodedb.NewNodeDb(
		srv.schedulingConfig.PriorityClasses,
		srv.schedulingConfig.IndexedResources,
		srv.schedulingConfig.IndexedTaints,
		srv.schedulingConfig.IndexedNodeLabels,
		srv.schedulingConfig.WellKnownNodeTypes,
		srv.resourceListFactory,
	)
	if err != nil {
		return nil, err
	}

	txn := nodeDb.Txn(true)
	defer txn.Abort()
	for _, node := range nodes {
		dbNode, err := nodeFactory.FromSchedulerObjectsNode(node)
		if err != nil {
			return nil, err
		}
		if err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, dbNode); err != nil {
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
