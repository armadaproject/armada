package replay

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/oklog/ulid/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	log "github.com/armadaproject/armada/internal/common/logging"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/scheduler"
	"github.com/armadaproject/armada/internal/scheduler/adapters"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/priorityoverride"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	"github.com/armadaproject/armada/pkg/api"
)

type productionScheduler struct {
	clock     *clocktesting.FakeClock
	db        *jobdb.JobDb
	algo      *scheduling.FairSchedulingAlgo
	checker   *scheduler.SubmitChecker
	farm      staticFarm
	nodes     map[string]*schedulerobjects.Node
	queues    map[string]bool
	config    configuration.SchedulingConfig
	resources *internaltypes.ResourceListFactory
}

// NewProductionScheduler composes Armada's scheduler and submit checker with
// fixed healthy executors, static queues and one simulated policy clock.
func NewProductionScheduler(
	ctx context.Context, warmupUS int64, config configuration.SchedulingConfig,
	executors []*schedulerobjects.Executor, queues []*api.Queue,
) (*productionScheduler, error) {
	validate := validator.New()
	validate.RegisterStructValidation(configuration.SchedulingConfigValidation, configuration.SchedulingConfig{})
	if err := validate.Struct(config); err != nil {
		return nil, fmt.Errorf("scheduler configuration: %w", err)
	}
	if _, ok := config.PriorityClasses[config.DefaultPriorityClassName]; !ok {
		return nil, fmt.Errorf("unknown default priority class %q", config.DefaultPriorityClassName)
	}
	if config.AsyncSchedulingEnabled || config.RetryPolicy.Enabled || config.RespectNodePodLimits || len(config.FloatingResources) > 0 {
		return nil, errors.New("replay does not support asynchronous scheduling, retry policies, pod-slot limits or floating resources")
	}
	if len(config.Pools) == 0 {
		return nil, errors.New("replay requires at least one configured scheduler pool")
	}
	for name, class := range config.PriorityClasses {
		if len(class.AwayNodeTypes) > 0 {
			return nil, fmt.Errorf("priority class %q enables away-node placement", name)
		}
	}
	pools := make(map[string]bool, len(config.Pools))
	for _, pool := range config.Pools {
		if pool.Name == "" || pools[pool.Name] {
			return nil, fmt.Errorf("empty or duplicate pool name %q", pool.Name)
		}
		pools[pool.Name] = true
		if len(pool.AwayPools) > 0 || pool.ShortJobPenaltyCutoff != 0 ||
			(pool.ExperimentalOptimiser != nil && pool.ExperimentalOptimiser.Enabled) ||
			(pool.ExperimentalMarketScheduling != nil && pool.ExperimentalMarketScheduling.Enabled) ||
			(pool.ExperimentalRunReconciliation != nil && pool.ExperimentalRunReconciliation.Enabled) {
			return nil, fmt.Errorf("pool %q enables away-pool placement, short-job penalties, optimiser, market scheduling or run reconciliation", pool.Name)
		}
	}

	config.MaxNewJobSchedulingDuration = 0
	config.MaxNewJobSchedulingDurationPerQueue = 0
	factory, err := internaltypes.NewResourceListFactory(config.SupportedResourceTypes, nil)
	if err != nil {
		return nil, err
	}
	floating, err := floatingresources.NewFloatingResourceTypes(nil, factory)
	if err != nil {
		return nil, err
	}
	clock := clocktesting.NewFakeClock(time.UnixMicro(warmupUS).UTC())
	farm := make(staticFarm, 0, len(executors))
	nodes := make(map[string]*schedulerobjects.Node)
	executorIDs := make(map[string]bool)
	totals := make(map[string]map[string]resource.Quantity)
	for _, source := range executors {
		if source == nil || source.Id == "" || executorIDs[source.Id] || !pools[source.Pool] {
			return nil, errors.New("farm requires unique executor IDs and configured executor pools")
		}
		executorIDs[source.Id] = true
		executor := *source
		executor.Nodes = make([]*schedulerobjects.Node, 0, len(source.Nodes))
		executor.LastUpdateTime = protoutil.ToTimestamp(clock.Now())
		for _, node := range source.Nodes {
			if node == nil || node.Id == "" || nodes[node.Id] != nil || node.Executor != executor.Id || !pools[node.Pool] {
				return nil, errors.New("farm requires unique node IDs, matching executor IDs and configured node pools")
			}
			if node.TotalResources == nil {
				return nil, fmt.Errorf("node %q has no allocatable resources", node.Id)
			}
			requests := make(map[string]resource.Quantity, len(node.TotalResources.Resources))
			for name, capacity := range node.TotalResources.Resources {
				if capacity == nil {
					return nil, fmt.Errorf("node %q resource %q has no quantity", node.Id, name)
				}
				requests[name] = *capacity
			}
			if err := checkExactResources(factory, requests); err != nil {
				return nil, fmt.Errorf("node %q capacity: %w", node.Id, err)
			}
			if totals[node.Pool] == nil {
				totals[node.Pool] = make(map[string]resource.Quantity)
			}
			for name, quantity := range requests {
				total := totals[node.Pool][name]
				total.Add(quantity)
				totals[node.Pool][name] = total
			}
			if len(node.StateByJobRunId) > 0 {
				return nil, fmt.Errorf("farm node %q must start empty", node.Id)
			}
			copy := *node
			copy.StateByJobRunId = make(map[string]schedulerobjects.JobRunState)
			nodes[node.Id] = &copy
			executor.Nodes = append(executor.Nodes, &copy)
		}
		if len(executor.UnassignedJobRuns) > 0 {
			return nil, fmt.Errorf("farm executor %q must start empty", executor.Id)
		}
		farm = append(farm, &executor)
	}
	for pool, resources := range totals {
		if err := checkExactResources(factory, resources); err != nil {
			return nil, fmt.Errorf("pool %q aggregate capacity: %w", pool, err)
		}
	}
	queueNames := make(map[string]bool, len(queues))
	cache := make(staticQueues, 0, len(queues))
	for _, queue := range queues {
		if queue == nil || queue.Name == "" || queueNames[queue.Name] ||
			math.IsNaN(queue.PriorityFactor) || math.IsInf(queue.PriorityFactor, 0) || queue.PriorityFactor <= 0 {
			return nil, errors.New("replay requires unique queue names and positive finite priority factors")
		}
		if err := validateQueueLimits(queue, config, pools); err != nil {
			return nil, err
		}
		queueNames[queue.Name] = true
		cache = append(cache, queue)
	}

	algo, err := scheduling.NewFairSchedulingAlgo(config, 0, farm, cache, nil, factory, floating,
		priorityoverride.NewNoOpProvider(), nil, scheduling.NewRunNodeReconciler(config.Pools), scheduling.WithPolicyClock(clock))
	if err != nil {
		return nil, err
	}
	checker := scheduler.NewSubmitChecker(config, configuration.SubmitCheckConfig{}, farm, cache, floating, factory)
	if err := checker.Initialise(replayContext(ctx)); err != nil {
		return nil, err
	}
	db := jobdb.NewJobDb(config.PriorityClasses, config.DefaultPriorityClassName, stringinterner.New(4096), factory)
	db.SetClock(clock)
	db.SetJobRunIDProvider(&runIDs{})
	db.SetAggregateEnabled(config.ExperimentalAggregateDemand.Compare || config.ExperimentalAggregateDemand.Use)
	return &productionScheduler{clock, db, algo, checker, farm, nodes, queueNames, config, factory}, nil
}

func (s *productionScheduler) advance(nowUS int64) time.Time {
	now := time.UnixMicro(nowUS).UTC()
	if !s.clock.Now().Equal(now) {
		s.clock.SetTime(now)
	}
	return now
}

func (s *productionScheduler) makeJob(offered Job) (*jobdb.Job, error) {
	if !s.queues[offered.Queue] {
		return nil, fmt.Errorf("unknown queue %q", offered.Queue)
	}
	if _, ok := s.config.PriorityClasses[offered.PriorityClass]; !ok {
		return nil, fmt.Errorf("unknown priority class %q", offered.PriorityClass)
	}
	if err := checkExactResources(s.resources, adapters.K8sResourceListToMap(offered.Requests)); err != nil {
		return nil, err
	}
	if offered.SubmittedUS < -maxDurationUS || offered.SubmittedUS > maxDurationUS {
		return nil, errors.New("job submission exceeds Armada nanosecond timestamp range")
	}
	submitted := time.UnixMicro(offered.SubmittedUS).UTC()
	for _, toleration := range offered.Tolerations {
		if toleration.TolerationSeconds != nil {
			return nil, errors.New("timed tolerations are outside the static replay profile")
		}
		switch toleration.Operator {
		case "", v1.TolerationOpEqual:
			if toleration.Key == "" {
				return nil, errors.New("Equal toleration requires a key")
			}
		case v1.TolerationOpExists:
			if toleration.Value != "" {
				return nil, errors.New("Exists toleration must have an empty value")
			}
		default:
			return nil, fmt.Errorf("unknown toleration operator %q", toleration.Operator)
		}
		switch toleration.Effect {
		case "", v1.TaintEffectNoExecute, v1.TaintEffectNoSchedule:
		default:
			return nil, fmt.Errorf("unsupported toleration effect %q", toleration.Effect)
		}
	}
	pod := &internaltypes.PodRequirements{
		NodeSelector: offered.NodeSelector, Tolerations: offered.Tolerations,
		ResourceRequirements: v1.ResourceRequirements{Requests: offered.Requests},
	}
	if offered.RequiredNodeAffinity != nil {
		for _, term := range offered.RequiredNodeAffinity.NodeSelectorTerms {
			if len(term.MatchFields) != 0 {
				return nil, errors.New("required node affinity matchFields is outside the replay profile")
			}
		}
		if _, err := nodeaffinity.NewNodeSelector(offered.RequiredNodeAffinity); err != nil {
			return nil, fmt.Errorf("invalid required node affinity: %w", err)
		}
		pod.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: offered.RequiredNodeAffinity,
		}}
	}
	info := &internaltypes.JobSchedulingInfo{
		PriorityClass: offered.PriorityClass, SubmitTime: submitted, Priority: offered.QueuePriority, PodRequirements: pod,
	}
	return s.db.NewJob(offered.ID, offered.JobSet, offered.Queue, offered.QueuePriority, info,
		true, 0, false, false, false, submitted.UnixNano(), true, nil, 0)
}

func (s *productionScheduler) Admit(ctx context.Context, nowUS int64, offered Job) (string, error) {
	s.advance(nowUS)
	job, err := s.makeJob(offered)
	if err != nil {
		return "", err
	}
	pools, reason, err := s.checker.CheckJobEligibility(replayContext(ctx), job)
	if err != nil {
		return "", err
	}
	if len(pools) == 0 {
		if reason == "" {
			reason = "no eligible pool"
		}
		return reason, nil
	}
	slices.Sort(pools)
	txn := s.db.WriteTxn()
	defer txn.Abort()
	if err := txn.Upsert([]*jobdb.Job{job.WithPools(pools)}); err != nil {
		return "", err
	}
	txn.Commit()
	return "", nil
}

func (s *productionScheduler) Complete(ctx context.Context, nowUS int64, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := s.advance(nowUS)
	txn := s.db.WriteTxn()
	defer txn.Abort()
	job := txn.GetById(id)
	if job == nil || job.Queued() || job.LatestRun() == nil {
		return fmt.Errorf("no leased job %q to complete", id)
	}
	run := job.LatestRun().WithSucceeded(true).WithTerminatedTime(&now)
	if err := txn.Upsert([]*jobdb.Job{job.WithUpdatedRun(run).WithSucceeded(true)}); err != nil {
		return err
	}
	if err := txn.BatchDelete([]string{id}); err != nil {
		return err
	}
	txn.Commit()
	delete(s.nodes[run.NodeId()].StateByJobRunId, run.Id())
	return nil
}

func (s *productionScheduler) Schedule(ctx context.Context, nowUS int64) (Decisions, error) {
	var decisions Decisions
	if nowUS < -maxDurationUS || nowUS > maxDurationUS {
		return decisions, errors.New("round time exceeds Armada nanosecond timestamp range")
	}
	now := s.advance(nowUS)
	for _, executor := range s.farm {
		executor.LastUpdateTime = protoutil.ToTimestamp(now)
	}
	txn := s.db.WriteTxn()
	defer txn.Abort()
	result, err := s.algo.Schedule(replayContext(ctx), txn)
	if err != nil {
		return decisions, err
	}
	if err := checkPoolOutcomes(ctx, result); err != nil {
		return decisions, err
	}
	for _, preempted := range result.GetAllPreemptedJobs() {
		job := preempted.Job
		run := job.LatestRun()
		delete(s.nodes[run.NodeId()].StateByJobRunId, run.Id())
		// Retry the same logical job with its original demand. Run history lives
		// in the replay results and need not be retained in the scheduler index.
		queued, err := s.db.NewJob(job.Id(), job.Jobset(), job.Queue(), job.Priority(),
			job.JobSchedulingInfo(), true, job.QueuedVersion()+1, false, false, false,
			job.Created(), true, job.Pools(), 0)
		if err != nil {
			return decisions, err
		}
		if err := txn.BatchDelete([]string{job.Id()}); err != nil {
			return decisions, err
		}
		if err := txn.Upsert([]*jobdb.Job{queued}); err != nil {
			return decisions, err
		}
		decisions.Preempt = append(decisions.Preempt, Preemption{JobID: job.Id(), Reason: string(preempted.PreemptionType)})
	}
	for _, scheduled := range result.GetAllScheduledJobs() {
		job := scheduled.Job
		run := job.LatestRun()
		if run == nil || run.Created() != now.UnixNano() || run.LeaseTime() == nil || !run.LeaseTime().Equal(now) {
			return decisions, fmt.Errorf("job %q lease does not match simulated round time", job.Id())
		}
		node := s.nodes[run.NodeId()]
		if node == nil {
			return decisions, fmt.Errorf("job %q scheduled on unknown node %q", job.Id(), run.NodeId())
		}
		// A fixed, healthy executor acknowledges and starts the allocated pod
		// immediately. Startup occupies the pod before useful work can advance.
		run = run.WithLeased(true).WithRunning(true).WithRunningTime(&now)
		if err := txn.Upsert([]*jobdb.Job{job.WithUpdatedRun(run)}); err != nil {
			return decisions, err
		}
		node.StateByJobRunId[run.Id()] = schedulerobjects.JobRunState_RUNNING
		decisions.Allocate = append(decisions.Allocate, Placement{
			JobID: job.Id(), RunID: run.Id(), Pool: node.Pool, SchedulingPool: run.Pool(), Node: run.NodeId(),
		})
	}
	txn.Commit()
	return decisions, nil
}

func checkPoolOutcomes(ctx context.Context, result *scheduling.SchedulerResult) error {
	if result == nil {
		return errors.New("scheduler returned no round result")
	}
	for _, pool := range result.PoolResults {
		switch pool.Outcome.TerminationReason() {
		case scheduling.PoolSchedulingTerminationReasonTimeout, scheduling.PoolSchedulingTerminationReasonSoftTimeout:
			if cause := errors.Join(ctx.Err(), pool.Outcome.Error()); cause != nil {
				return fmt.Errorf("pool %q timed out (%s): %w", pool.Name, pool.Outcome.TerminationReason(), cause)
			}
			return fmt.Errorf("pool %q timed out (%s)", pool.Name, pool.Outcome.TerminationReason())
		}
		if !pool.Outcome.Success() {
			return fmt.Errorf("pool %q: %w", pool.Name, pool.Outcome.Error())
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func replayContext(ctx context.Context) *armadacontext.Context {
	return armadacontext.New(ctx, log.NullLogger)
}

func checkExactResources(factory *internaltypes.ResourceListFactory, requests map[string]resource.Quantity) error {
	converted, err := factory.FromJobResourceListFailOnUnknown(requests)
	if err != nil {
		return err
	}
	for name, requested := range requests {
		if requested.Sign() < 0 || requested.Cmp(converted.GetByNameZeroIfMissing(name)) != 0 {
			return fmt.Errorf("resource %q quantity %s cannot be represented exactly as a nonnegative scheduler resource", name, requested.String())
		}
	}
	return nil
}

func validateQueueLimits(queue *api.Queue, cfg configuration.SchedulingConfig, pools map[string]bool) error {
	resources := make(map[string]bool, len(cfg.SupportedResourceTypes))
	for _, resource := range cfg.SupportedResourceTypes {
		resources[resource.Name] = true
	}
	check := func(fractions map[string]float64) error {
		for name, fraction := range fractions {
			if !resources[name] || math.IsNaN(fraction) || fraction < 0 || fraction > 1 {
				return fmt.Errorf("queue %q contains an invalid resource limit for %q", queue.Name, name)
			}
		}
		return nil
	}
	for name, limits := range queue.ResourceLimitsByPriorityClassName {
		if _, known := cfg.PriorityClasses[name]; !known || limits == nil {
			return fmt.Errorf("queue %q has unknown priority class or null limits for %q", queue.Name, name)
		}
		if err := check(limits.MaximumResourceFraction); err != nil {
			return err
		}
		for pool, override := range limits.MaximumResourceFractionByPool {
			if !pools[pool] || override == nil {
				return fmt.Errorf("queue %q has unknown pool or null limits for %q", queue.Name, pool)
			}
			if err := check(override.MaximumResourceFraction); err != nil {
				return err
			}
		}
	}
	return nil
}

type staticQueues []*api.Queue

func (q staticQueues) GetAll(_ *armadacontext.Context) ([]*api.Queue, error) { return q, nil }

type staticFarm []*schedulerobjects.Executor

func (f staticFarm) GetExecutors(_ *armadacontext.Context) ([]*schedulerobjects.Executor, error) {
	return f, nil
}

func (f staticFarm) GetExecutorSettings(_ *armadacontext.Context) ([]*schedulerobjects.ExecutorSettings, error) {
	return nil, nil
}

func (f staticFarm) GetLastUpdateTimes(_ *armadacontext.Context) (map[string]time.Time, error) {
	times := make(map[string]time.Time, len(f))
	for _, executor := range f {
		times[executor.Id] = protoutil.ToStdTime(executor.LastUpdateTime)
	}
	return times, nil
}

func (f staticFarm) StoreExecutor(_ *armadacontext.Context, _ *schedulerobjects.Executor) error {
	return errors.New("cannot replace a fixed replay executor")
}

type runIDs struct {
	last ulid.ULID
}

func (ids *runIDs) New() string {
	for i := len(ids.last) - 1; i >= 0; i-- {
		ids.last[i]++
		if ids.last[i] != 0 {
			break
		}
	}
	return ids.last.String()
}
