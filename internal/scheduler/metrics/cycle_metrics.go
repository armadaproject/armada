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
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	"github.com/armadaproject/armada/pkg/metricevents"
)

var (
	poolLabels                             = []string{poolLabel}
	poolAndPriorityLabels                  = []string{poolLabel, priorityLabel}
	poolAndQueueLabels                     = []string{poolLabel, queueLabel}
	poolAndQueueAndPriorityClassTypeLabels = []string{poolLabel, queueLabel, priorityClassLabel, typeLabel}
	poolQueueAndResourceLabels             = []string{poolLabel, queueLabel, resourceLabel}
	defaultType                            = "unknown"
)

type perCycleMetrics struct {
	consideredJobs               *prometheus.GaugeVec
	fairShare                    *prometheus.GaugeVec
	adjustedFairShare            *prometheus.GaugeVec
	uncappedAdjustedFairShare    *prometheus.GaugeVec
	actualShare                  *prometheus.GaugeVec
	fairnessError                *prometheus.GaugeVec
	demand                       *prometheus.GaugeVec
	constrainedDemand            *prometheus.GaugeVec
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
		[]string{poolLabel, nodeLabel, clusterLabel, nodeTypeLabel, resourceLabel, "schedulable"},
	)

	nodeAllocatedResource := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "node_allocated_resource",
			Help: "Resource allocated by Armada jobs on this node",
		},
		[]string{poolLabel, nodeLabel, clusterLabel, nodeTypeLabel, resourceLabel, "schedulable"},
	)

	return &perCycleMetrics{
		consideredJobs:               consideredJobs,
		fairShare:                    fairShare,
		adjustedFairShare:            adjustedFairShare,
		uncappedAdjustedFairShare:    uncappedAdjustedFairShare,
		actualShare:                  actualShare,
		demand:                       demand,
		constrainedDemand:            constrainedDemand,
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
		spotPrice:                    spotPrice,
		indicativeShare:              indicativeShare,
		nodePreemptibility:           nodePreemptibility,
		protectedFractionOfFairShare: protectedFractionOfFairShare,
		nodeAllocatableResource:      nodeAllocatableResource,
		nodeAllocatedResource:        nodeAllocatedResource,
	}
}

type cycleMetrics struct {
	leaderMetricsEnabled bool

	scheduledJobs           *prometheus.CounterVec
	premptedJobs            *prometheus.CounterVec
	scheduleCycleTime       prometheus.Histogram
	reconciliationCycleTime prometheus.Histogram
	latestCycleMetrics      atomic.Pointer[perCycleMetrics]
	metricsPublisher        pulsarutils.Publisher[*metricevents.Event]
}

func newCycleMetrics(publisher pulsarutils.Publisher[*metricevents.Event]) *cycleMetrics {
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

	cycleMetrics := &cycleMetrics{
		leaderMetricsEnabled:    true,
		scheduledJobs:           scheduledJobs,
		premptedJobs:            premptedJobs,
		scheduleCycleTime:       scheduleCycleTime,
		reconciliationCycleTime: reconciliationCycleTime,
		latestCycleMetrics:      atomic.Pointer[perCycleMetrics]{},
		metricsPublisher:        publisher,
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
	m.latestCycleMetrics.Store(newPerCycleMetrics())
}

func (m *cycleMetrics) ReportScheduleCycleTime(cycleTime time.Duration) {
	m.scheduleCycleTime.Observe(float64(cycleTime.Milliseconds()))
}

func (m *cycleMetrics) ReportReconcileCycleTime(cycleTime time.Duration) {
	m.reconciliationCycleTime.Observe(float64(cycleTime.Milliseconds()))
}

func (m *cycleMetrics) ReportSchedulerResult(ctx *armadacontext.Context, result scheduling.SchedulerResult) {
	// Metrics that depend on pool
	currentCycle := newPerCycleMetrics()
	for _, schedContext := range result.SchedulingContexts {
		pool := schedContext.Pool
		for queue, queueContext := range schedContext.QueueSchedulingContexts {
			jobsConsidered := float64(len(queueContext.UnsuccessfulJobSchedulingContexts) + len(queueContext.SuccessfulJobSchedulingContexts))
			actualShare := schedContext.FairnessCostProvider.UnweightedCostFromQueue(queueContext)
			demand := schedContext.FairnessCostProvider.UnweightedCostFromAllocation(queueContext.Demand)
			constrainedDemand := schedContext.FairnessCostProvider.UnweightedCostFromAllocation(queueContext.ConstrainedDemand)

			currentCycle.consideredJobs.WithLabelValues(pool, queue).Set(jobsConsidered)
			currentCycle.fairShare.WithLabelValues(pool, queue).Set(queueContext.FairShare)
			currentCycle.adjustedFairShare.WithLabelValues(pool, queue).Set(queueContext.DemandCappedAdjustedFairShare)
			currentCycle.uncappedAdjustedFairShare.WithLabelValues(pool, queue).Set(queueContext.UncappedAdjustedFairShare)
			currentCycle.actualShare.WithLabelValues(pool, queue).Set(actualShare)
			currentCycle.demand.WithLabelValues(pool, queue).Set(demand)
			currentCycle.constrainedDemand.WithLabelValues(pool, queue).Set(constrainedDemand)
			currentCycle.queueWeight.WithLabelValues(pool, queue).Set(queueContext.Weight)
			currentCycle.rawQueueWeight.WithLabelValues(pool, queue).Set(queueContext.RawWeight)
		}
		currentCycle.fairnessError.WithLabelValues(pool).Set(schedContext.FairnessError())
		currentCycle.spotPrice.WithLabelValues(pool).Set(schedContext.SpotPrice)
		for priority, share := range schedContext.ExperimentalIndicativeShares {
			currentCycle.indicativeShare.WithLabelValues(pool, strconv.Itoa(priority)).Set(share)
		}
	}

	for pool, schedulingStats := range result.PerPoolSchedulingStats {
		for queue, s := range schedulingStats.StatsPerQueue {
			currentCycle.gangsConsidered.WithLabelValues(pool, queue).Set(float64(s.GangsConsidered))
			currentCycle.gangsScheduled.WithLabelValues(pool, queue).Set(float64(s.GangsScheduled))
			currentCycle.firstGangQueuePosition.WithLabelValues(pool, queue).Set(float64(s.FirstGangConsideredQueuePosition))
			currentCycle.lastGangQueuePosition.WithLabelValues(pool, queue).Set(float64(s.LastGangScheduledQueuePosition))
			currentCycle.perQueueCycleTime.WithLabelValues(pool, queue).Set(float64(s.Time.Milliseconds()))
		}

		for _, jobCtx := range schedulingStats.ScheduledJobs {
			schedulingType := defaultType
			if jobCtx.PodSchedulingContext != nil && jobCtx.PodSchedulingContext.SchedulingMethod != "" {
				schedulingType = string(jobCtx.PodSchedulingContext.SchedulingMethod)
			}
			m.scheduledJobs.WithLabelValues(pool, jobCtx.Job.Queue(), jobCtx.PriorityClassName, schedulingType).Inc()
		}

		for _, jobCtx := range schedulingStats.PreemptedJobs {
			preemptionType := defaultType
			if jobCtx.PreemptionType != "" {
				preemptionType = string(jobCtx.PreemptionType)
			}
			m.premptedJobs.WithLabelValues(pool, jobCtx.Job.Queue(), jobCtx.PriorityClassName, preemptionType).Inc()
		}

		currentCycle.loopNumber.WithLabelValues(pool).Set(float64(schedulingStats.LoopNumber))

		for queue, s := range schedulingStats.EvictorResult.GetStatsPerQueue() {
			currentCycle.evictedJobs.WithLabelValues(pool, queue).Set(float64(s.EvictedJobCount))

			for _, r := range s.EvictedResources.GetResources() {
				currentCycle.evictedResources.WithLabelValues(pool, queue, r.Name).Set(float64(r.RawValue))
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
			for _, node := range nodes {
				isSchedulable := strconv.FormatBool(!node.IsUnschedulable())
				for _, resource := range node.GetAllocatableResources().GetResources() {
					currentCycle.nodeAllocatableResource.WithLabelValues(node.GetPool(), node.GetName(), node.GetExecutor(), node.GetReportingNodeType(), resource.Name, isSchedulable).Set(armadaresource.QuantityAsFloat64(resource.Value))
				}

				allocated := node.GetAllocatableResources().Subtract(node.AllocatableByPriority[internaltypes.EvictedPriority])
				for _, resource := range allocated.GetResources() {
					allocatableValue := math.Max(armadaresource.QuantityAsFloat64(resource.Value), 0)
					currentCycle.nodeAllocatedResource.WithLabelValues(node.GetPool(), node.GetName(), node.GetExecutor(), node.GetReportingNodeType(), resource.Name, isSchedulable).Set(allocatableValue)
				}
			}
		}

		currentCycle.protectedFractionOfFairShare.WithLabelValues(pool).Set(schedulingStats.ProtectedFractionOfFairShare)
	}
	m.latestCycleMetrics.Store(currentCycle)

	m.publishCycleMetrics(ctx, result)
}

func (m *cycleMetrics) describe(ch chan<- *prometheus.Desc) {
	if m.leaderMetricsEnabled {
		m.scheduledJobs.Describe(ch)
		m.premptedJobs.Describe(ch)
		m.scheduleCycleTime.Describe(ch)

		currentCycle := m.latestCycleMetrics.Load()
		currentCycle.consideredJobs.Describe(ch)
		currentCycle.fairShare.Describe(ch)
		currentCycle.adjustedFairShare.Describe(ch)
		currentCycle.uncappedAdjustedFairShare.Describe(ch)
		currentCycle.actualShare.Describe(ch)
		currentCycle.fairnessError.Describe(ch)
		currentCycle.demand.Describe(ch)
		currentCycle.constrainedDemand.Describe(ch)
		currentCycle.queueWeight.Describe(ch)
		currentCycle.rawQueueWeight.Describe(ch)
		currentCycle.gangsConsidered.Describe(ch)
		currentCycle.gangsScheduled.Describe(ch)
		currentCycle.firstGangQueuePosition.Describe(ch)
		currentCycle.lastGangQueuePosition.Describe(ch)
		currentCycle.perQueueCycleTime.Describe(ch)
		currentCycle.loopNumber.Describe(ch)
		currentCycle.evictedJobs.Describe(ch)
		currentCycle.evictedResources.Describe(ch)
		currentCycle.spotPrice.Describe(ch)
		currentCycle.indicativeShare.Describe(ch)
		currentCycle.nodePreemptibility.Describe(ch)
		currentCycle.protectedFractionOfFairShare.Describe(ch)
		currentCycle.nodeAllocatableResource.Describe(ch)
		currentCycle.nodeAllocatedResource.Describe(ch)
	}

	m.reconciliationCycleTime.Describe(ch)
}

func (m *cycleMetrics) collect(ch chan<- prometheus.Metric) {
	if m.leaderMetricsEnabled {
		m.scheduledJobs.Collect(ch)
		m.premptedJobs.Collect(ch)
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
		currentCycle.spotPrice.Collect(ch)
		currentCycle.indicativeShare.Collect(ch)
		currentCycle.nodePreemptibility.Collect(ch)
		currentCycle.protectedFractionOfFairShare.Collect(ch)
		currentCycle.nodeAllocatableResource.Collect(ch)
		currentCycle.nodeAllocatedResource.Collect(ch)
	}

	m.reconciliationCycleTime.Collect(ch)
}

func (m *cycleMetrics) publishCycleMetrics(ctx *armadacontext.Context, result scheduling.SchedulerResult) {
	events := make([]*metricevents.Event, len(result.SchedulingContexts))

	// convenience function to convert k8s qty struct to pointers as demanded by proto
	toQtyPtr := func(q resource.Quantity) *resource.Quantity {
		return &q
	}

	for i, sc := range result.SchedulingContexts {
		queueMetrics := make(map[string]*metricevents.QueueMetrics, len(sc.QueueSchedulingContexts))
		for qName, qCtx := range sc.QueueSchedulingContexts {
			queueMetrics[qName] = &metricevents.QueueMetrics{
				ActualShare:          sc.FairnessCostProvider.UnweightedCostFromQueue(qCtx),
				Demand:               sc.FairnessCostProvider.UnweightedCostFromAllocation(qCtx.Demand),
				ConstrainedDemand:    sc.FairnessCostProvider.UnweightedCostFromAllocation(qCtx.ConstrainedDemand),
				DemandByResourceType: armadamaps.MapValues(qCtx.Demand.ToMap(), toQtyPtr),
			}
		}
		events[i] = &metricevents.Event{
			Created: protoutil.ToTimestamp(sc.Finished),
			Event: &metricevents.Event_CycleMetrics{CycleMetrics: &metricevents.CycleMetrics{
				Pool:                 sc.Pool,
				QueueMetrics:         queueMetrics,
				AllocatableResources: armadamaps.MapValues(sc.TotalResources.ToMap(), toQtyPtr),
			}},
		}
	}
	err := m.metricsPublisher.PublishMessages(ctx, events...)
	if err != nil {
		ctx.Logger().WithError(err).Warn("Error publishing cycle metrics")
	}
}
