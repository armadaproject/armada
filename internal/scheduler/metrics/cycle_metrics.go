package metrics

import (
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	log "github.com/armadaproject/armada/internal/common/logging"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/pkg/metricevents"
)

var (
	poolLabels                             = []string{poolLabel}
	poolAndPriorityLabels                  = []string{poolLabel, priorityLabel}
	poolAndQueueLabels                     = []string{poolLabel, queueLabel}
	poolAndQueueAndPriorityClassTypeLabels = []string{poolLabel, queueLabel, priorityClassLabel, typeLabel}
	poolAndShapeLabels                     = []string{poolLabel, jobShapeLabel}
	poolAndShapeAndReasonLabels            = []string{poolLabel, jobShapeLabel, unschedulableReasonLabel}
	poolQueueAndResourceLabels             = []string{poolLabel, queueLabel, resourceLabel}
	poolAndOutcomeLabels                   = []string{poolLabel, outcomeLabel, terminationReasonLabel}
	defaultType                            = "unknown"
)

type perCycleMetrics struct {
	consideredJobs               *prometheus.GaugeVec
	fairShare                    *prometheus.GaugeVec
	adjustedFairShare            *prometheus.GaugeVec
	uncappedAdjustedFairShare    *prometheus.GaugeVec
	actualShare                  *prometheus.GaugeVec
	billableResource             *prometheus.GaugeVec
	fairnessError                *prometheus.GaugeVec
	demand                       *prometheus.GaugeVec
	constrainedDemand            *prometheus.GaugeVec
	shortJobPenalty              *prometheus.GaugeVec
	queueWeight                  *prometheus.GaugeVec
	rawQueueWeight               *prometheus.GaugeVec
	gangsConsidered              *prometheus.GaugeVec
	gangsScheduled               *prometheus.GaugeVec
	firstGangQueuePosition       *prometheus.GaugeVec
	lastGangQueuePosition        *prometheus.GaugeVec
	perQueueCycleTime            *prometheus.GaugeVec
	loopNumber                   *prometheus.GaugeVec
	evictedJobs                  *prometheus.GaugeVec
	evictedResources             *prometheus.GaugeVec
	spotPrice                    *prometheus.GaugeVec
	indicativeShare              *prometheus.GaugeVec
	nodePreemptibility           *prometheus.GaugeVec
	protectedFractionOfFairShare *prometheus.GaugeVec
	nodeAllocatableResource      *prometheus.GaugeVec
	nodeAllocatedResource        *prometheus.GaugeVec
	indicativePrice              *prometheus.GaugeVec
	indicativePriceSchedulable   *prometheus.GaugeVec
	idealisedScheduledValue      *prometheus.GaugeVec
	idealisedAllocatedResource   *prometheus.GaugeVec
	realisedScheduledValue       *prometheus.GaugeVec
	nodePoolSize                 *prometheus.GaugeVec
}

func newPerCycleMetrics() *perCycleMetrics {
	consideredJobs := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "considered_jobs",
			Help: "Number of jobs considered",
		},
		poolAndQueueLabels,
	)

	fairShare := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "fair_share",
			Help: "Fair share of each queue",
		},
		poolAndQueueLabels,
	)

	adjustedFairShare := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "adjusted_fair_share",
			Help: "Adjusted Fair share of each queue",
		},
		poolAndQueueLabels,
	)

	uncappedAdjustedFairShare := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "uncapped_adjusted_fair_share",
			Help: "Adjusted Fair share of each queue, not capped by demand",
		},
		poolAndQueueLabels,
	)

	actualShare := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "actual_share",
			Help: "Actual Fair share of each queue",
		},
		poolAndQueueLabels,
	)

	demand := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "demand",
			Help: "Demand of each queue",
		},
		poolAndQueueLabels,
	)

	constrainedDemand := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "constrained_demand",
			Help: "Constrained demand of each queue and pool.  This differs from demand in that it limits demand by scheduling constraints",
		},
		poolAndQueueLabels,
	)

	shortJobPenalty := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "short_job_penalty",
			Help: "Short job penalty for each queue. This is the resource used by jobs that started recently and exited soon after they started.",
		},
		poolAndQueueLabels,
	)

	queueWeight := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "queue_weight",
			Help: "Weight of the queue after multipliers have been applied",
		},
		poolAndQueueLabels,
	)

	rawQueueWeight := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "raw_queue_weight",
			Help: "Weight of the queue before multipliers have been applied",
		},
		poolAndQueueLabels,
	)

	fairnessError := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "fairness_error",
			Help: "Cumulative delta between adjusted fair share and actual share for all users who are below their fair share",
		},
		[]string{poolLabel},
	)

	gangsConsidered := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "gangs_considered",
			Help: "Number of gangs considered in this scheduling cycle",
		},
		poolAndQueueLabels,
	)

	gangsScheduled := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "gangs_scheduled",
			Help: "Number of gangs scheduled in this scheduling cycle",
		},
		poolAndQueueLabels,
	)

	firstGangQueuePosition := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "first_gang_queue_position",
			Help: "First position in the scheduling loop where a gang was considered",
		},
		poolAndQueueLabels,
	)

	lastGangQueuePosition := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "last_gang_queue_position",
			Help: "Last position in the scheduling loop where a gang was considered",
		},
		poolAndQueueLabels,
	)

	perQueueCycleTime := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "per_queue_schedule_cycle_times",
			Help: "Per queue cycle time when in a scheduling round.",
		},
		poolAndQueueLabels,
	)

	loopNumber := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "loop_number",
			Help: "Number of scheduling loops in this cycle",
		},
		poolLabels,
	)

	evictedJobs := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "evicted_jobs",
			Help: "Number of jobs evicted in this cycle",
		},
		poolAndQueueLabels,
	)

	evictedResources := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "evicted_resources",
			Help: "Resources evicted in this cycle",
		},
		poolQueueAndResourceLabels,
	)

	billableResources := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "billable_resources",
			Help: "Resources billable for in this cycle",
		},
		poolQueueAndResourceLabels,
	)

	spotPrice := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "spot_price",
			Help: "spot price for given pool",
		},
		poolLabels,
	)

	indicativeShare := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "indicative_share",
			Help: "indicative share for some priority in the given pool",
		},
		poolAndPriorityLabels,
	)

	nodePreemptibility := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "node_preemptibility",
			Help: "is it possible to clear this node by preempting any jobs on it?",
		},
		[]string{poolLabel, nodeLabel, clusterLabel, nodeTypeLabel, "isPreemptible", "reason"},
	)

	protectedFractionOfFairShare := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "protected_fraction_of_fair_share",
			Help: "config value protectedFractionOfFairShare - will evict preemptible jobs if actual_share / max(fair_share, adjusted_fair_share) is greater than this",
		},
		[]string{poolLabel},
	)

	nodeAllocatableResource := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "node_allocatable_resource",
			Help: "Resource that can be allocated to Armada jobs on this node",
		},
		[]string{poolLabel, nodeLabel, clusterLabel, nodeTypeLabel, resourceLabel, reservationLabel, "schedulable", "overAllocated"},
	)

	nodeAllocatedResource := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "node_allocated_resource",
			Help: "Resource allocated by Armada jobs on this node",
		},
		[]string{poolLabel, nodeLabel, clusterLabel, nodeTypeLabel, resourceLabel, reservationLabel, "schedulable", "overAllocated"},
	)

	indicativePrice := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "indicative_price",
			Help: "indicative price for configured job in pool",
		},
		poolAndShapeLabels,
	)

	indicativePriceSchedulable := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "indicative_price_is_schedulable",
			Help: "determines whether defined job is at all schedulable",
		},
		poolAndShapeAndReasonLabels,
	)

	idealisedScheduledValue := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "idealised_scheduled_value",
			Help: "Idealised value scheduled per queue",
		},
		poolAndQueueLabels,
	)

	idealisedAllocatedResource := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "idealised_allocated_resource",
			Help: "Idealised value scheduled per queue",
		},
		[]string{poolLabel, queueLabel, resourceLabel},
	)

	realisedScheduledValue := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "realised_scheduled_value",
			Help: "Realised value scheduled per queue",
		},
		poolAndQueueLabels,
	)

	nodePoolSize := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "node_pool_size",
			Help: "Number of nodes in each pool",
		},
		poolLabels,
	)

	return &perCycleMetrics{
		consideredJobs:               consideredJobs,
		fairShare:                    fairShare,
		adjustedFairShare:            adjustedFairShare,
		uncappedAdjustedFairShare:    uncappedAdjustedFairShare,
		actualShare:                  actualShare,
		demand:                       demand,
		constrainedDemand:            constrainedDemand,
		shortJobPenalty:              shortJobPenalty,
		queueWeight:                  queueWeight,
		rawQueueWeight:               rawQueueWeight,
		fairnessError:                fairnessError,
		gangsConsidered:              gangsConsidered,
		gangsScheduled:               gangsScheduled,
		firstGangQueuePosition:       firstGangQueuePosition,
		lastGangQueuePosition:        lastGangQueuePosition,
		perQueueCycleTime:            perQueueCycleTime,
		loopNumber:                   loopNumber,
		evictedJobs:                  evictedJobs,
		evictedResources:             evictedResources,
		billableResource:             billableResources,
		spotPrice:                    spotPrice,
		indicativeShare:              indicativeShare,
		nodePreemptibility:           nodePreemptibility,
		protectedFractionOfFairShare: protectedFractionOfFairShare,
		nodeAllocatableResource:      nodeAllocatableResource,
		nodeAllocatedResource:        nodeAllocatedResource,
		indicativePrice:              indicativePrice,
		indicativePriceSchedulable:   indicativePriceSchedulable,
		idealisedScheduledValue:      idealisedScheduledValue,
		idealisedAllocatedResource:   idealisedAllocatedResource,
		realisedScheduledValue:       realisedScheduledValue,
		nodePoolSize:                 nodePoolSize,
	}
}

type cycleMetrics struct {
	leaderMetricsEnabled bool

	scheduledJobs           *prometheus.CounterVec
	premptedJobs            *prometheus.CounterVec
	poolSchedulingOutcome   *prometheus.CounterVec
	scheduleCycleTime       prometheus.Histogram
	reconciliationCycleTime prometheus.Histogram
	latestCycleMetrics      atomic.Pointer[perCycleMetrics]
	metricsPublisher        pulsarutils.Publisher[*metricevents.Event]
	poolNames               []string
}

func newCycleMetrics(publisher pulsarutils.Publisher[*metricevents.Event], poolNames []string) *cycleMetrics {
	scheduledJobs := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "scheduled_jobs",
			Help: "Number of events scheduled",
		},
		poolAndQueueAndPriorityClassTypeLabels,
	)

	premptedJobs := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "preempted_jobs",
			Help: "Number of jobs preempted",
		},
		poolAndQueueAndPriorityClassTypeLabels,
	)

	scheduleCycleTime := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    prefix + "schedule_cycle_times",
			Help:    "Cycle time when in a scheduling round.",
			Buckets: prometheus.ExponentialBuckets(10.0, 1.1, 110),
		},
	)

	reconciliationCycleTime := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    prefix + "reconciliation_cycle_times",
			Help:    "Cycle time when in a scheduling round.",
			Buckets: prometheus.ExponentialBuckets(10.0, 1.1, 110),
		},
	)

	poolSchedulingOutcome := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "pool_scheduling_outcome",
			Help: "Number of scheduling attempts per pool by outcome",
		},
		poolAndOutcomeLabels,
	)

	cycleMetrics := &cycleMetrics{
		leaderMetricsEnabled:    true,
		scheduledJobs:           scheduledJobs,
		premptedJobs:            premptedJobs,
		poolSchedulingOutcome:   poolSchedulingOutcome,
		scheduleCycleTime:       scheduleCycleTime,
		reconciliationCycleTime: reconciliationCycleTime,
		latestCycleMetrics:      atomic.Pointer[perCycleMetrics]{},
		metricsPublisher:        publisher,
		poolNames:               poolNames,
	}
	cycleMetrics.latestCycleMetrics.Store(newPerCycleMetrics())
	return cycleMetrics
}

func (m *cycleMetrics) enableLeaderMetrics() {
	m.leaderMetricsEnabled = true
}

func (m *cycleMetrics) disableLeaderMetrics() {
	m.resetLeaderMetrics()
	m.leaderMetricsEnabled = false
}

func (m *cycleMetrics) resetLeaderMetrics() {
	m.premptedJobs.Reset()
	m.scheduledJobs.Reset()
	m.poolSchedulingOutcome.Reset()
	m.latestCycleMetrics.Store(newPerCycleMetrics())
}

func (m *cycleMetrics) ReportScheduleCycleTime(cycleTime time.Duration) {
	m.scheduleCycleTime.Observe(float64(cycleTime.Milliseconds()))
}

func (m *cycleMetrics) ReportReconcileCycleTime(cycleTime time.Duration) {
	m.reconciliationCycleTime.Observe(float64(cycleTime.Milliseconds()))
}

func (m *cycleMetrics) ReportJobPreemptedWithType(job *jobdb.Job, preemptionType context.PreemptionType) {
	if job.LatestRun() == nil {
		return
	}
	m.premptedJobs.WithLabelValues(job.LatestRun().Pool(), job.Queue(), job.PriorityClassName(), string(preemptionType)).Inc()
}

func (m *cycleMetrics) ReportPoolSchedulingOutcomes(outcome scheduling.PoolSchedulingOutcome) {
	terminationReason := string(outcome.TerminationReason)
	result := PoolSchedulingOutcomeSuccess
	if !outcome.Success {
		result = PoolSchedulingOutcomeFailure
	}
	m.poolSchedulingOutcome.WithLabelValues(outcome.Pool, result, terminationReason).Inc()
}

func (m *cycleMetrics) ReportSchedulerResult(ctx *armadacontext.Context, result scheduling.SchedulerResult) {
	currentCycle := newPerCycleMetrics()

	for _, poolResult := range result.PoolResults {
		m.ReportPoolSchedulingOutcomes(poolResult.Outcome)
		if poolResult.ReconciliationResult != nil {
			for _, info := range poolResult.ReconciliationResult.FailedJobs {
				m.ReportJobPreemptedWithType(info.Job, context.PreemptedViaNodeReconciler)
			}
			for _, info := range poolResult.ReconciliationResult.PreemptedJobs {
				m.ReportJobPreemptedWithType(info.Job, context.PreemptedViaNodeReconciler)
			}
		}

		if poolResult.SchedulingResult != nil {
			schedulingResult := poolResult.SchedulingResult

			pool := poolResult.Name
			schedContext := schedulingResult.SchedulingContext
			if schedContext != nil {
				for queue, queueContext := range schedContext.QueueSchedulingContexts {
					jobsConsidered := float64(len(queueContext.UnsuccessfulJobSchedulingContexts) + len(queueContext.SuccessfulJobSchedulingContexts))
					actualShare := schedContext.FairnessCostProvider.UnweightedCostFromAllocation(queueContext.GetAllocation())
					demand := schedContext.FairnessCostProvider.UnweightedCostFromAllocation(queueContext.Demand)
					constrainedDemand := schedContext.FairnessCostProvider.UnweightedCostFromAllocation(queueContext.ConstrainedDemand)
					shortJobPenalty := schedContext.FairnessCostProvider.UnweightedCostFromAllocation(queueContext.ShortJobPenalty)

					currentCycle.consideredJobs.WithLabelValues(pool, queue).Set(jobsConsidered)
					currentCycle.fairShare.WithLabelValues(pool, queue).Set(queueContext.FairShare)
					currentCycle.adjustedFairShare.WithLabelValues(pool, queue).Set(queueContext.DemandCappedAdjustedFairShare)
					currentCycle.uncappedAdjustedFairShare.WithLabelValues(pool, queue).Set(queueContext.UncappedAdjustedFairShare)
					currentCycle.actualShare.WithLabelValues(pool, queue).Set(actualShare)
					currentCycle.demand.WithLabelValues(pool, queue).Set(demand)
					currentCycle.constrainedDemand.WithLabelValues(pool, queue).Set(constrainedDemand)
					currentCycle.shortJobPenalty.WithLabelValues(pool, queue).Set(shortJobPenalty)
					currentCycle.queueWeight.WithLabelValues(pool, queue).Set(queueContext.Weight)
					currentCycle.rawQueueWeight.WithLabelValues(pool, queue).Set(queueContext.RawWeight)
					currentCycle.idealisedScheduledValue.WithLabelValues(pool, queue).Set(queueContext.IdealisedValue)
					currentCycle.realisedScheduledValue.WithLabelValues(pool, queue).Set(queueContext.RealisedValue)
					for _, r := range queueContext.GetBillableResource().GetAll() {
						currentCycle.billableResource.WithLabelValues(pool, queue, r.Name).Set(r.Value.AsApproximateFloat64())
					}
					for _, r := range queueContext.IdealisedAllocated.GetAll() {
						currentCycle.idealisedAllocatedResource.WithLabelValues(pool, queue, r.Name).Set(r.Value.AsApproximateFloat64())
					}
				}
				currentCycle.fairnessError.WithLabelValues(pool).Set(schedContext.FairnessError())
				currentCycle.spotPrice.WithLabelValues(pool).Set(schedContext.GetSpotPrice())
				for priority, share := range schedContext.ExperimentalIndicativeShares {
					currentCycle.indicativeShare.WithLabelValues(pool, strconv.Itoa(priority)).Set(share)
				}
			}

			schedulingStats := schedulingResult.AdditionalSchedulingInfo

			if schedulingStats != nil {
				for queue, s := range schedulingStats.StatsPerQueue {
					currentCycle.gangsConsidered.WithLabelValues(pool, queue).Set(float64(s.GangsConsidered))
					currentCycle.gangsScheduled.WithLabelValues(pool, queue).Set(float64(s.GangsScheduled))
					currentCycle.firstGangQueuePosition.WithLabelValues(pool, queue).Set(float64(s.FirstGangConsideredQueuePosition))
					currentCycle.lastGangQueuePosition.WithLabelValues(pool, queue).Set(float64(s.LastGangScheduledQueuePosition))
					currentCycle.perQueueCycleTime.WithLabelValues(pool, queue).Set(float64(s.Time.Milliseconds()))
				}

				currentCycle.loopNumber.WithLabelValues(pool).Set(float64(schedulingStats.LoopNumber))

				for queue, s := range schedulingStats.EvictorResult.GetStatsPerQueue() {
					currentCycle.evictedJobs.WithLabelValues(pool, queue).Set(float64(s.EvictedJobCount))

					for _, r := range s.EvictedResources.GetAll() {
						currentCycle.evictedResources.WithLabelValues(pool, queue, r.Name).Set(r.Value.AsApproximateFloat64())
					}
				}

				for _, nodePreemptiblityStats := range schedulingStats.EvictorResult.NodePreemptiblityStats {
					currentCycle.nodePreemptibility.WithLabelValues(
						pool,
						nodePreemptiblityStats.NodeName,
						nodePreemptiblityStats.Cluster,
						nodePreemptiblityStats.NodeType,
						fmt.Sprintf("%t", nodePreemptiblityStats.Preemptible),
						nodePreemptiblityStats.Reason).Set(1.0)
				}

				nodes, err := schedulingStats.NodeDb.GetNodes()
				if err != nil {
					log.Errorf("unable to generate node stats as failed to get nodes from nodeDb %s", err)
				} else {
					currentCycle.nodePoolSize.WithLabelValues(pool).Set(float64(len(nodes)))
					for _, node := range nodes {
						isSchedulable := strconv.FormatBool(!node.IsUnschedulable())
						isOverallocated := strconv.FormatBool(node.IsOverAllocated())
						for _, resource := range node.GetAllocatableResources().GetAll() {
							currentCycle.nodeAllocatableResource.WithLabelValues(node.GetPool(), node.GetName(), node.GetExecutor(), node.GetReportingNodeType(), resource.Name, node.GetReservation(), isSchedulable, isOverallocated).Set(resource.Value.AsApproximateFloat64())
						}

						allocated := node.GetAllocatableResources().Subtract(node.AllocatableByPriority[internaltypes.EvictedPriority])
						for _, resource := range allocated.GetAll() {
							allocatableValue := math.Max(resource.Value.AsApproximateFloat64(), 0)
							currentCycle.nodeAllocatedResource.WithLabelValues(node.GetPool(), node.GetName(), node.GetExecutor(), node.GetReportingNodeType(), resource.Name, node.GetReservation(), isSchedulable, isOverallocated).Set(allocatableValue)
						}
					}
				}

				currentCycle.protectedFractionOfFairShare.WithLabelValues(pool).Set(schedulingStats.ProtectedFractionOfFairShare)

				for name, pricingInfo := range schedulingStats.MarketDrivenIndicativePrices {
					if pricingInfo.Evaluated {
						currentCycle.indicativePrice.WithLabelValues(pool, name).Set(pricingInfo.Price)
						if pricingInfo.Schedulable {
							currentCycle.indicativePriceSchedulable.WithLabelValues(pool, name, pricingInfo.UnschedulableReason).Set(1.0)
						} else {
							currentCycle.indicativePriceSchedulable.WithLabelValues(pool, name, pricingInfo.UnschedulableReason).Set(0.0)
						}
					} else {
						currentCycle.indicativePrice.WithLabelValues(pool, name).Set(math.NaN())
						currentCycle.indicativePriceSchedulable.WithLabelValues(pool, name, pricingInfo.UnschedulableReason).Set(math.NaN())
					}
				}
			}

			for _, jobCtx := range schedulingResult.ScheduledJobs {
				schedulingType := defaultType
				if jobCtx.PodSchedulingContext != nil && jobCtx.PodSchedulingContext.SchedulingMethod != "" {
					schedulingType = string(jobCtx.PodSchedulingContext.SchedulingMethod)
				}
				m.scheduledJobs.WithLabelValues(pool, jobCtx.Job.Queue(), jobCtx.Job.PriorityClassName(), schedulingType).Inc()
			}

			for _, jobCtx := range schedulingResult.PreemptedJobs {
				preemptionType := defaultType
				if jobCtx.PreemptionType != "" {
					preemptionType = string(jobCtx.PreemptionType)
				}
				m.premptedJobs.WithLabelValues(pool, jobCtx.Job.Queue(), jobCtx.Job.PriorityClassName(), preemptionType).Inc()
			}
		}
	}

	m.latestCycleMetrics.Store(currentCycle)

	m.publishCycleMetrics(ctx, result)
}

func (m *cycleMetrics) describe(ch chan<- *prometheus.Desc) {
	if m.leaderMetricsEnabled {
		m.scheduledJobs.Describe(ch)
		m.premptedJobs.Describe(ch)
		m.poolSchedulingOutcome.Describe(ch)
		m.scheduleCycleTime.Describe(ch)

		cycleMetrics := newPerCycleMetrics()
		cycleMetrics.consideredJobs.Describe(ch)
		cycleMetrics.fairShare.Describe(ch)
		cycleMetrics.adjustedFairShare.Describe(ch)
		cycleMetrics.uncappedAdjustedFairShare.Describe(ch)
		cycleMetrics.actualShare.Describe(ch)
		cycleMetrics.fairnessError.Describe(ch)
		cycleMetrics.demand.Describe(ch)
		cycleMetrics.constrainedDemand.Describe(ch)
		cycleMetrics.shortJobPenalty.Describe(ch)
		cycleMetrics.queueWeight.Describe(ch)
		cycleMetrics.rawQueueWeight.Describe(ch)
		cycleMetrics.gangsConsidered.Describe(ch)
		cycleMetrics.gangsScheduled.Describe(ch)
		cycleMetrics.firstGangQueuePosition.Describe(ch)
		cycleMetrics.lastGangQueuePosition.Describe(ch)
		cycleMetrics.perQueueCycleTime.Describe(ch)
		cycleMetrics.loopNumber.Describe(ch)
		cycleMetrics.evictedJobs.Describe(ch)
		cycleMetrics.evictedResources.Describe(ch)
		cycleMetrics.billableResource.Describe(ch)
		cycleMetrics.spotPrice.Describe(ch)
		cycleMetrics.indicativeShare.Describe(ch)
		cycleMetrics.nodePreemptibility.Describe(ch)
		cycleMetrics.protectedFractionOfFairShare.Describe(ch)
		cycleMetrics.nodeAllocatableResource.Describe(ch)
		cycleMetrics.nodeAllocatedResource.Describe(ch)
		cycleMetrics.indicativePrice.Describe(ch)
		cycleMetrics.indicativePriceSchedulable.Describe(ch)
		cycleMetrics.idealisedScheduledValue.Describe(ch)
		cycleMetrics.idealisedAllocatedResource.Describe(ch)
		cycleMetrics.realisedScheduledValue.Describe(ch)
		cycleMetrics.nodePoolSize.Describe(ch)
	}

	m.reconciliationCycleTime.Describe(ch)
}

func (m *cycleMetrics) collect(ch chan<- prometheus.Metric) {
	if m.leaderMetricsEnabled {
		m.scheduledJobs.Collect(ch)
		m.premptedJobs.Collect(ch)
		m.poolSchedulingOutcome.Collect(ch)
		m.scheduleCycleTime.Collect(ch)

		currentCycle := m.latestCycleMetrics.Load()
		currentCycle.consideredJobs.Collect(ch)
		currentCycle.fairShare.Collect(ch)
		currentCycle.adjustedFairShare.Collect(ch)
		currentCycle.uncappedAdjustedFairShare.Collect(ch)
		currentCycle.actualShare.Collect(ch)
		currentCycle.fairnessError.Collect(ch)
		currentCycle.demand.Collect(ch)
		currentCycle.constrainedDemand.Collect(ch)
		currentCycle.shortJobPenalty.Collect(ch)
		currentCycle.rawQueueWeight.Collect(ch)
		currentCycle.queueWeight.Collect(ch)
		currentCycle.gangsConsidered.Collect(ch)
		currentCycle.gangsScheduled.Collect(ch)
		currentCycle.firstGangQueuePosition.Collect(ch)
		currentCycle.lastGangQueuePosition.Collect(ch)
		currentCycle.perQueueCycleTime.Collect(ch)
		currentCycle.loopNumber.Collect(ch)
		currentCycle.evictedJobs.Collect(ch)
		currentCycle.evictedResources.Collect(ch)
		currentCycle.billableResource.Collect(ch)
		currentCycle.spotPrice.Collect(ch)
		currentCycle.indicativeShare.Collect(ch)
		currentCycle.nodePreemptibility.Collect(ch)
		currentCycle.protectedFractionOfFairShare.Collect(ch)
		currentCycle.nodeAllocatableResource.Collect(ch)
		currentCycle.nodeAllocatedResource.Collect(ch)
		currentCycle.indicativePrice.Collect(ch)
		currentCycle.indicativePriceSchedulable.Collect(ch)
		currentCycle.idealisedScheduledValue.Collect(ch)
		currentCycle.idealisedAllocatedResource.Collect(ch)
		currentCycle.realisedScheduledValue.Collect(ch)
		currentCycle.nodePoolSize.Collect(ch)
	}

	m.reconciliationCycleTime.Collect(ch)
}

func (m *cycleMetrics) publishCycleMetrics(ctx *armadacontext.Context, result scheduling.SchedulerResult) {
	events := make([]*metricevents.Event, len(result.PoolResults))

	// convenience function to convert k8s qty struct to pointers as demanded by proto
	toQtyPtr := func(q resource.Quantity) *resource.Quantity {
		return &q
	}

	for i, pr := range result.PoolResults {
		if pr.GetSchedulingContext() == nil {
			continue
		}
		sc := pr.GetSchedulingContext()
		queueMetrics := make(map[string]*metricevents.QueueMetrics, len(sc.QueueSchedulingContexts))
		for qName, qCtx := range sc.QueueSchedulingContexts {
			queueMetrics[qName] = &metricevents.QueueMetrics{
				ActualShare:                      sc.FairnessCostProvider.UnweightedCostFromAllocation(qCtx.GetAllocation()),
				Demand:                           sc.FairnessCostProvider.UnweightedCostFromAllocation(qCtx.Demand),
				ConstrainedDemand:                sc.FairnessCostProvider.UnweightedCostFromAllocation(qCtx.ConstrainedDemand),
				DemandByResourceType:             armadamaps.MapValues(qCtx.Demand.ToMap(), toQtyPtr),
				ConstrainedDemandByResourceType:  armadamaps.MapValues(qCtx.ConstrainedDemand.ToMap(), toQtyPtr),
				ShortJobPenalty:                  sc.FairnessCostProvider.UnweightedCostFromAllocation(qCtx.ShortJobPenalty),
				BillableAllocationByResourceType: armadamaps.MapValues(qCtx.GetBillableResource().ToMap(), toQtyPtr),
			}
		}
		events[i] = &metricevents.Event{
			Created: protoutil.ToTimestamp(sc.Finished),
			Event: &metricevents.Event_CycleMetrics{CycleMetrics: &metricevents.CycleMetrics{
				Pool:                 sc.Pool,
				QueueMetrics:         queueMetrics,
				AllocatableResources: armadamaps.MapValues(sc.TotalResources.ToMap(), toQtyPtr),
				SpotPrice:            sc.GetSpotPrice(),
				CycleTime:            protoutil.ToTimestamp(sc.Finished),
			}},
		}
	}
	err := m.metricsPublisher.PublishMessages(ctx, events...)
	if err != nil {
		ctx.Logger().WithError(err).Warn("Error publishing cycle metrics")
	}
}
