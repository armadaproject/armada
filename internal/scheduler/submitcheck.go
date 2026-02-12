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
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/queue"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
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
	constraintsByPool         map[string]constraints.SchedulingConstraints
	jobSchedulingResultsCache *lru.Cache
}

type SubmitScheduleChecker interface {
	Check(ctx *armadacontext.Context, jobs []*jobdb.Job) (map[string]schedulingResult, error)
}

type SubmitChecker struct {
	schedulingConfig       configuration.SchedulingConfig
	poolsBySubmissionGroup map[string][]string
	executorRepository     database.ExecutorRepository
	queueCache             queue.QueueCache
	floatingResourceTypes  *floatingresources.FloatingResourceTypes
	resourceListFactory    *internaltypes.ResourceListFactory
	state                  atomic.Pointer[schedulerState]
	clock                  clock.Clock // can  be  overridden for testing
}

func NewSubmitChecker(
	schedulingConfig configuration.SchedulingConfig,
	executorRepository database.ExecutorRepository,
	queueCache queue.QueueCache,
	floatingResourceTypes *floatingresources.FloatingResourceTypes,
	resourceListFactory *internaltypes.ResourceListFactory,
) *SubmitChecker {
	poolsBySubmissionGroup := map[string][]string{}
	for _, pool := range schedulingConfig.Pools {
		if _, exists := poolsBySubmissionGroup[pool.GetSubmissionGroup()]; !exists {
			poolsBySubmissionGroup[pool.GetSubmissionGroup()] = []string{}
		}
		poolsBySubmissionGroup[pool.GetSubmissionGroup()] = append(poolsBySubmissionGroup[pool.GetSubmissionGroup()], pool.Name)
	}
	return &SubmitChecker{
		schedulingConfig:       schedulingConfig,
		poolsBySubmissionGroup: poolsBySubmissionGroup,
		executorRepository:     executorRepository,
		queueCache:             queueCache,
		floatingResourceTypes:  floatingResourceTypes,
		resourceListFactory:    resourceListFactory,
		clock:                  clock.RealClock{},
	}
}

func (srv *SubmitChecker) Initialise(ctx *armadacontext.Context) error {
	err := srv.updateExecutors(ctx)
	if err != nil {
		ctx.Logger().WithStacktrace(err).Errorf("Error initialising submit checker")
	}

	return err
}

func (srv *SubmitChecker) Run(ctx *armadacontext.Context) error {
	ctx.Infof("Will refresh executor state every %s", srv.schedulingConfig.ExecutorUpdateFrequency)
	if err := srv.updateExecutors(ctx); err != nil {
		ctx.Logger().WithStacktrace(err).Error("Failed updating executors")
	}

	ticker := time.NewTicker(srv.schedulingConfig.ExecutorUpdateFrequency)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := srv.updateExecutors(ctx); err != nil {
				ctx.Logger().WithStacktrace(err).Error("Failed updating executors")
			}
		}
	}
}

func (srv *SubmitChecker) updateExecutors(ctx *armadacontext.Context) error {
	queues, err := srv.queueCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("failed fetching queues from queue cache - %s", err)
	}
	executors, err := srv.executorRepository.GetExecutors(ctx)
	if err != nil {
		return fmt.Errorf("failed fetching executors from db - %s", err)
	}

	ctx.Infof("Retrieved %d executors", len(executors))
	jobSchedulingResultsCache, err := lru.New(10000)
	if err != nil {
		// This should never happen as lru.New only returns an error if it is initialised with a negative size
		panic(err)
	}

	nodeFactory := internaltypes.NewNodeFactory(
		srv.schedulingConfig.IndexedTaints,
		srv.schedulingConfig.IndexedNodeLabels,
		srv.schedulingConfig.PriorityClasses,
		srv.resourceListFactory)

	executorsByPoolAndId := map[string]map[string]*executor{}
	totalResourcesByPool := map[string]internaltypes.ResourceList{}
	for _, ex := range executors {
		nodes := nodeFactory.FromSchedulerObjectsExecutors(
			[]*schedulerobjects.Executor{ex},
			func(s string) { ctx.Error(s) })
		nodesByPool := armadaslices.GroupByFunc(nodes, func(n *internaltypes.Node) string {
			return n.GetPool()
		})
		for pool, nodes := range nodesByPool {

			nodeDb, err := srv.constructNodeDb(nodes)

			if _, present := executorsByPoolAndId[pool]; !present {
				executorsByPoolAndId[pool] = map[string]*executor{}
			}

			if err == nil {
				totalResourcesByPool[pool] = totalResourcesByPool[pool].Add(nodeDb.TotalKubernetesResources())

				executorsByPoolAndId[pool][ex.Id] = &executor{
					id:     ex.Id,
					nodeDb: nodeDb,
				}
			} else {
				ctx.Logger().
					WithStacktrace(err).
					Warnf("Error constructing nodedb for executor: %s", ex.Id)
			}
		}
	}

	for _, pool := range srv.floatingResourceTypes.AllPools() {
		totalResourcesByPool[pool] = totalResourcesByPool[pool].Add(srv.floatingResourceTypes.GetTotalAvailableForPool(pool))
	}

	constraintsByPool := map[string]constraints.SchedulingConstraints{}
	for pool, totalResources := range totalResourcesByPool {
		constraintsByPool[pool] = constraints.NewSchedulingConstraints(
			pool,
			totalResources,
			srv.schedulingConfig,
			queues,
		)
	}

	srv.state.Store(&schedulerState{
		executorsByPoolAndId:      executorsByPoolAndId,
		constraintsByPool:         constraintsByPool,
		jobSchedulingResultsCache: jobSchedulingResultsCache,
	})

	return nil
}

func (srv *SubmitChecker) getPoolsBySubmissionGroup(submissionGroup string) []string {
	return srv.poolsBySubmissionGroup[submissionGroup]
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
	for _, job := range jobs {
		results[job.Id()] = srv.getIndividualSchedulingResult(job, state)
	}

	// Then, check if all gangs can be scheduled.
	for gangId, jctxs := range armadaslices.GroupByFunc(
		jobContexts,
		func(jctx *context.JobSchedulingContext) string {
			return jctx.Job.GetGangInfo().Id()
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

func (srv *SubmitChecker) getIndividualSchedulingResult(job *jobdb.Job, state *schedulerState) schedulingResult {
	// Mark this job context as "not in a gang" for the individual scheduling check.
	job = job.WithGangInfo(jobdb.BasicJobGangInfo())
	jctx := context.JobSchedulingContextFromJob(job)
	schedulingKey := jctx.Job.SchedulingKey()

	if obj, ok := state.jobSchedulingResultsCache.Get(schedulingKey); ok {
		return obj.(schedulingResult)
	}

	gctx := context.NewGangSchedulingContext([]*context.JobSchedulingContext{jctx})
	result := srv.getSchedulingResult(gctx, state)

	state.jobSchedulingResultsCache.Add(schedulingKey, result)

	return result
}

// Check if a set of jobs can be scheduled onto some cluster.
// TODO: there are a number of things this won't catch:
//   - Node Uniformity Label (although it will work if this is per cluster)
//   - Gang jobs that will use more than the allowed capacity limit
func (srv *SubmitChecker) getSchedulingResult(originalGangCtx *context.GangSchedulingContext, state *schedulerState) schedulingResult {
	successfulPools := map[string]bool{}
	var sb strings.Builder

poolStart:
	for _, pool := range srv.schedulingConfig.Pools {

		if successfulPools[pool.Name] {
			continue
		}

		for _, awayPool := range pool.AwayPools {
			if successfulPools[awayPool] {
				continue poolStart
			}
		}

		if originalGangCtx.RequestsFloatingResources {
			rr := originalGangCtx.TotalResourceRequests
			if ok, reason := srv.floatingResourceTypes.WithinLimits(pool.Name, rr); !ok {
				sb.WriteString(fmt.Sprintf("pool %s:\n", pool.Name))
				sb.WriteString(fmt.Sprintf("job/gang requests floating resources %s but %s\n", rr.OfType(internaltypes.Floating).String(), reason))
				sb.WriteString("\n---\n")
				continue
			}
		}

		c := state.constraintsByPool[pool.Name]
		if c != nil {
			queueLimit := c.GetQueueResourceLimit(originalGangCtx.Queue, originalGangCtx.PriorityClassName())
			if !queueLimit.IsEmpty() && originalGangCtx.TotalResourceRequests.Exceeds(queueLimit) {
				sb.WriteString(fmt.Sprintf("pool %s:\n", pool.Name))
				sb.WriteString(fmt.Sprintf("job/gang requests resources %s which exceeds the total limit of %s for its queue/priority class\n", originalGangCtx.TotalResourceRequests, queueLimit))
				sb.WriteString("\n---\n")
				continue
			}
		}

		executors := maps.Values(state.executorsByPoolAndId[pool.Name])
		for _, awayPool := range pool.AwayPools {
			executors = append(executors, maps.Values(state.executorsByPoolAndId[awayPool])...)
		}

		for _, ex := range executors {

			// copy the gctx here, as we are going to mutate it
			gctx := copyGangContext(originalGangCtx)

			// TODO construct nodedb per synthetic pool to avoid needing to set this dynamically
			if pool.DisableAwayScheduling {
				ex.nodeDb.DisableAwayScheduling()
			} else {
				ex.nodeDb.EnableAwayScheduling()
			}

			if pool.DisableHomeScheduling {
				ex.nodeDb.DisableHomeScheduling()
			} else {
				ex.nodeDb.EnableHomeScheduling()
			}

			if pool.DisableGangAwayScheduling {
				ex.nodeDb.DisableGangAwayScheduling()
			} else {
				ex.nodeDb.EnableGangAwayScheduling()
			}

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
					for _, pool := range srv.getPoolsBySubmissionGroup(pool.GetSubmissionGroup()) {
						successfulPools[pool] = true
					}
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
						": %d out of %d pods schedulable\n",
						numSuccessfullyScheduled, len(gctx.JobSchedulingContexts),
					),
				)
			}
		}
	}
	if len(successfulPools) > 0 {
		return schedulingResult{isSchedulable: true, pools: maps.Keys(successfulPools)}
	}
	return schedulingResult{isSchedulable: false, reason: sb.String()}
}

func (srv *SubmitChecker) constructNodeDb(nodes []*internaltypes.Node) (*nodedb.NodeDb, error) {
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
		if err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node); err != nil {
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

func copyGangContext(gctx *context.GangSchedulingContext) *context.GangSchedulingContext {
	jctxs := make([]*context.JobSchedulingContext, len(gctx.JobSchedulingContexts))
	for i, jctx := range gctx.JobSchedulingContexts {
		jctxs[i] = context.JobSchedulingContextFromJob(jctx.Job)
	}
	return context.NewGangSchedulingContext(jctxs)
}
