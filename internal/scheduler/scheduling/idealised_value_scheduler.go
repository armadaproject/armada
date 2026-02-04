package scheduling

import (
	"slices"

	"github.com/google/uuid"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

const (
	gangUniformityLabel = "mega-node-gang-uniformity-label"
	gangUniformityValue = "mega-node"
)

type IdealisedValueScheduler struct {
	schedulingContext     *schedulercontext.SchedulingContext
	constraints           schedulerconstraints.SchedulingConstraints
	floatingResourceTypes *floatingresources.FloatingResourceTypes
	schedulingConfig      configuration.SchedulingConfig
	nodeDb                *nodedb.NodeDb
	jobRepo               jobdb.JobRepository
	runningJobs           []*jobdb.Job
}

// NewIdealisedValueScheduler returns a new IdealisedValueScheduler. This is a scheduler that schedules jobs in a
// market-driven way on a theoretical "mega node" which contains all of our cluster resources.
// This allows us to calculate the maximum possible value we could achieve if we were not constrained by node boundaries
// which is a useful metric as the user does not know about these node boundaries and therefore would expect this value
// of their jobs to be realised.  By calculating this we can therefore track the "expectation gap" between the value
// that a user expects to see and a value that we actually achieve.
func NewIdealisedValueScheduler(
	sctx *schedulercontext.SchedulingContext,
	constraints schedulerconstraints.SchedulingConstraints,
	floatingResourceTypes *floatingresources.FloatingResourceTypes,
	config configuration.SchedulingConfig,
	rlf *internaltypes.ResourceListFactory,
	nodes []*internaltypes.Node,
	jobRepo jobdb.JobRepository,
	runningJobs []*jobdb.Job,
) (*IdealisedValueScheduler, error) {
	// Create a nodedb with a single, mythical, node that contains all resources in the pool
	megaNode := createMegaNode(sctx.Pool, nodes, config, rlf)
	nodeDb, err := createNodeDb(config, rlf, megaNode)
	if err != nil {
		return nil, err
	}

	return &IdealisedValueScheduler{
		schedulingContext:     sctx,
		constraints:           constraints,
		floatingResourceTypes: floatingResourceTypes,
		schedulingConfig:      config,
		nodeDb:                nodeDb,
		jobRepo:               jobRepo,
		runningJobs:           runningJobs,
	}, nil
}

func (sch *IdealisedValueScheduler) Schedule(ctx *armadacontext.Context) (*SchedulingResult, error) {
	pool := sch.schedulingContext.Pool
	sctx := sch.schedulingContext

	// Create job iterators that contain both running and evicted jobs
	runningJobCtxs := make([]*schedulercontext.JobSchedulingContext, len(sch.runningJobs))
	for i, job := range sch.runningJobs {
		runningJobCtxs[i] = schedulercontext.JobSchedulingContextFromJob(job)
	}
	inMemoryJobRepo := NewInMemoryJobRepository(pool, jobdb.MarketJobPriorityComparer{Pool: pool})
	inMemoryJobRepo.EnqueueMany(runningJobCtxs)
	jobIteratorsByQueue := make(map[string]JobContextIterator)
	for _, qctx := range sctx.QueueSchedulingContexts {
		runningIt := NewStaticRequirementsIgnoringIterator(inMemoryJobRepo.GetJobIterator(qctx.Queue))
		queueIt := NewStaticRequirementsIgnoringIterator(NewQueuedJobsIterator(ctx, qctx.Queue, sctx.Pool, jobdb.PriceOrder, sch.jobRepo))
		jobIteratorsByQueue[qctx.Queue] = NewMarketDrivenMultiJobsIterator(sctx.Pool, runningIt, queueIt)
	}

	// Run  a scheduling round
	qs, err := NewQueueScheduler(
		sch.schedulingContext,
		sch.constraints,
		sch.floatingResourceTypes,
		sch.nodeDb,
		jobIteratorsByQueue,
		false,
		false,
		false,
		sch.schedulingConfig.MaxQueueLookback,
		true,
		1.0)
	if err != nil {
		return nil, err
	}
	return qs.Schedule(ctx)
}

func createNodeDb(schedulingConfig configuration.SchedulingConfig, rlf *internaltypes.ResourceListFactory, node *internaltypes.Node) (*nodedb.NodeDb, error) {
	nodeDb, err := nodedb.NewNodeDb(
		schedulingConfig.PriorityClasses,
		schedulingConfig.IndexedResources,
		schedulingConfig.IndexedTaints,
		slices.Concat([]string{gangUniformityLabel}, schedulingConfig.IndexedNodeLabels),
		schedulingConfig.WellKnownNodeTypes,
		rlf,
	)
	if err != nil {
		return nil, err
	}
	txn := nodeDb.Txn(true)
	err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node)
	if err != nil {
		txn.Abort()
		return nil, err
	}
	txn.Commit()
	return nodeDb, nil
}

func createMegaNode(pool string, nodes []*internaltypes.Node, schedulingConfig configuration.SchedulingConfig, rlf *internaltypes.ResourceListFactory) *internaltypes.Node {
	totalResources := rlf.MakeAllZero()
	allocatableResources := rlf.MakeAllZero()
	priorityClasses := make(map[int32]bool)
	allocatableByPriority := make(map[int32]internaltypes.ResourceList)

	nf := internaltypes.NewNodeFactory(
		schedulingConfig.IndexedTaints,
		schedulingConfig.IndexedNodeLabels,
		schedulingConfig.PriorityClasses,
		rlf,
	)

	for _, node := range nodes {
		if !node.IsUnschedulable() {
			totalResources = totalResources.Add(node.GetTotalResources())
			allocatableResources = allocatableResources.Add(node.GetAllocatableResources())
			for priority := range node.AllocatableByPriority {
				priorityClasses[priority] = true
			}
		}
	}

	for priority := range priorityClasses {
		allocatableByPriority[priority] = allocatableResources
	}

	node := nf.CreateNodeAndType(
		uuid.NewString(),
		"idealised-value-executor",
		"mega-node-1",
		pool,
		"mega-node",
		false,
		[]v1.Taint{},
		map[string]string{gangUniformityLabel: gangUniformityValue},
		totalResources,
		allocatableResources,
		allocatableByPriority)
	return node
}

func NewStaticRequirementsIgnoringIterator(iter JobContextIterator) JobContextIterator {
	return staticRequirementsIgnoringIterator{
		iter: iter,
	}
}

type staticRequirementsIgnoringIterator struct {
	iter JobContextIterator
}

func (s staticRequirementsIgnoringIterator) Next() (*schedulercontext.JobSchedulingContext, error) {
	next, err := s.iter.Next()
	if err != nil {
		return nil, err
	}
	if next == nil {
		return nil, nil
	}
	job := next.Job
	if job.IsInGang() {
		gangInfo := job.GetGangInfo()
		job = job.WithGangInfo(jobdb.CreateGangInfo(gangInfo.Id(), gangInfo.Cardinality(), gangUniformityLabel))
	}
	return &schedulercontext.JobSchedulingContext{
		Created:   next.Created,
		JobId:     next.JobId,
		IsEvicted: next.IsEvicted,
		Job:       job,
		PodRequirements: &internaltypes.PodRequirements{
			ResourceRequirements: next.PodRequirements.ResourceRequirements,
		},
		CurrentGangCardinality:         next.CurrentGangCardinality,
		KubernetesResourceRequirements: next.KubernetesResourceRequirements,
		AdditionalNodeSelectors:        next.AdditionalNodeSelectors,
		AdditionalTolerations:          next.AdditionalTolerations,
		UnschedulableReason:            next.UnschedulableReason,
		PodSchedulingContext:           next.PodSchedulingContext,
	}, nil
}
