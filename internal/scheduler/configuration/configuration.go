package configuration

import (
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	authconfig "github.com/armadaproject/armada/internal/common/auth/configuration"
	commonconfig "github.com/armadaproject/armada/internal/common/config"
	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
	profilingconfig "github.com/armadaproject/armada/internal/common/profiling/configuration"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/client"
)

const (
	// NodeIdLabel maps to a unique id associated with each node.
	// This label is automatically added to nodes within the NodeDb.
	NodeIdLabel = "armadaproject.io/nodeId"
)

type Configuration struct {
	// Database configuration
	Postgres configuration.PostgresConfig
	// Armada Api Connection.  Used to fetch queues.
	ArmadaApi client.ApiConnectionDetails
	// General Pulsar configuration
	Pulsar commonconfig.PulsarConfig
	// Configuration controlling leader election
	Leader LeaderConfig
	// Configuration controlling metrics
	Metrics MetricsConfig
	// Scheduler configuration (this is shared with the old scheduler)
	Scheduling SchedulingConfig
	Auth       authconfig.AuthConfig
	Grpc       grpcconfig.GrpcConfig
	Http       HttpConfig
	// If non-nil, configures pprof profiling
	Profiling *profilingconfig.ProfilingConfig
	// Maximum number of strings that should be cached at any one time
	InternedStringsCacheSize uint32 `validate:"required"`
	// How often the scheduling cycle should run
	CyclePeriod time.Duration `validate:"required"`
	// How often the job scheduling should run
	// This is expected to be a greater value than CyclePeriod as we don't need to schedule every cycle
	// This keeps the system more responsive as other operations happen in each cycle - such as state changes
	SchedulePeriod time.Duration `validate:"required"`
	// MaxSchedulingDuration is the hard timeout for a scheduling cycle.
	// When exceeded, the scheduler aborts immediately and returns an error,
	// discarding all uncommitted work from the current cycle.
	//
	// This is a safety limit to prevent runaway scheduling cycles from blocking
	// the system indefinitely.
	//
	// Must be greater than NewJobsSchedulingTimeout.
	MaxSchedulingDuration time.Duration `validate:"required"`
	// NewJobsSchedulingTimeout is the soft timeout for scheduling new jobs.
	// When exceeded, the scheduler stops considering new jobs and only
	// attempts to reschedule evicted jobs for the remainder of the cycle.
	//
	// This ensures evicted jobs (which were preempted mid-simulation) get
	// rescheduled before the cycle commits, while still bounding total cycle time.
	//
	// Set to 0 to disable (scheduler will schedule new jobs until hard timeout).
	// Must be less than MaxSchedulingDuration when non-zero.
	NewJobsSchedulingTimeout time.Duration
	// How long after a heartbeat an executor will be considered lost
	ExecutorTimeout time.Duration `validate:"required"`
	// Maximum number of rows to fetch in a given query
	DatabaseFetchSize int `validate:"required"`
	// Frequency at which queues will be fetched from the API
	QueueRefreshPeriod time.Duration `validate:"required"`
	// Allows queue priority overrides to be fetched from an external source.
	PriorityOverride PriorityOverrideConfig
	// Configuration for the pricing API
	// This is used to retrieve queue pricing information
	PricingApi PricingApiConfig
	// Whether to publish metrics To Pulsar.  This is currently experimental
	PublishMetricsToPulsar bool
}

type LeaderConfig struct {
	// Valid modes are "standalone" or "kubernetes"
	Mode string `validate:"required"`
	// Name of the K8s Lock Object
	LeaseLockName string
	// Namespace of the K8s Lock Object
	LeaseLockNamespace string
	// The name of the pod
	PodName string
	// How long the lease is held for.
	// Non leaders much wait this long before trying to acquire the lease
	LeaseDuration time.Duration
	// RenewDeadline is the duration that the acting leader will retry refreshing leadership before giving up.
	RenewDeadline time.Duration
	// RetryPeriod is the duration the LeaderElector clients should waite between tries of actions.
	RetryPeriod time.Duration
	// Connection details to the leader
	LeaderConnection client.ApiConnectionDetails
}

type FloatingResourceConfig struct {
	// Resource name, e.g. "storage-connections"
	Name string
	// Resolution with which Armada tracks this resource; larger values indicate lower resolution.
	Resolution resource.Quantity
	// Per-pool config.
	Pools []FloatingResourcePoolConfig
}

type FloatingResourcePoolConfig struct {
	// Name of the pool.
	Name string
	// Amount of this resource that can be allocated across all jobs in this pool.
	Quantity resource.Quantity
}
type HttpConfig struct {
	Port int `validate:"required"`
}

type MetricsConfig struct {
	Port                         uint16
	RefreshInterval              time.Duration
	JobStateMetricsResetInterval time.Duration
	// Used to calculate job seconds lost to preemption
	// Calculate as if the job checkpoints at these different intervals
	JobCheckpointIntervals []time.Duration
	// Regexes used for job error categorisation.
	// Specifically, the subCategory label for job failure counters is the first regex that matches the job error.
	// If no regex matches, the subCategory label is the empty string.
	TrackedErrorRegexes []string
	// Metrics are exported for these resources.
	TrackedResourceNames []v1.ResourceName
}

type HistogramConfig struct {
	Start  float64
	Factor float64
	Count  int
}

// SchedulingConfig contains config controlling the Armada scheduler.
//
// The Armada scheduler is in charge of assigning pods to cluster and nodes.
// The Armada scheduler is part of the Armada control plane.
//
// Features:
// 1. Queuing and fairly dividing resources between users.
// 2. Fair preemption, including between jobs of equal priority to balance resource allocation.
// 3. Gang scheduling, optional across clusters, and with lower and upper bounds on the number of jobs scheduled.
//
// Note that Armada still relies on kube-scheduler for binding of pods to nodes.
// This is achieved by adding to each pod created by Armada a node selector that matches only the intended node.
type SchedulingConfig struct {
	// Set to true to disable scheduling
	DisableScheduling bool
	// Set to true to make scheduling all or nothing
	// By default pools the scheduler will attempt fail scheduling independently, not causing other pools to also fail
	// This only applies to certain types of known failures
	//	- critical failures will still cause a total scheduling round failure
	DisableIndependentPoolFailures bool
	// Set to true to enable scheduler assertions. This results in some performance loss.
	EnableAssertions bool
	// Experimental
	// Set to true to enable larger job preferential ordering in the candidate gang iterator.
	// This will result in larger jobs being ordered earlier in the job scheduling order
	EnablePreferLargeJobOrdering bool
	// ProtectedFractionOfFairShare sets the threshold for fair-share preemption.
	// Queues at or below this fraction of their fair share are protected from preemption.
	//
	//   1.0 = Fair-share preemption disabled (default)
	//   0.5 = Queues over 50% of fair share can have jobs preempted
	//   0.1 = Aggressive - only queues under 10% of fair share are protected
	//
	// This only affects fair-share preemption, not priority-based preemption.
	// Can be overridden per-pool via Pools[].ProtectedFractionOfFairShare.
	ProtectedFractionOfFairShare float64 `validate:"gte=0"`
	// Armada adds a node selector term to every scheduled pod using this label with the node name as value.
	// This to force kube-scheduler to schedule pods on the node chosen by Armada.
	// For example, if NodeIdLabel is "kubernetes.io/hostname" and armada schedules a pod on node "myNode",
	// then Armada adds "kubernetes.io/hostname": "myNode" to the pod node selector before sending it to the executor.
	NodeIdLabel string `validate:"required"`
	// PriorityClasses defines Armada's own priority classes for scheduling and preemption.
	// These are separate from Kubernetes PriorityClass resources. Armada handles all preemption
	// in its scheduler before pods reach Kubernetes.
	//
	// Each class has two key fields:
	//   - priority: Higher values are scheduled first and can preempt lower-priority jobs
	//   - preemptible: If true, jobs can be evicted to rebalance resources across queues
	//
	// Example:
	//   priorityClasses:
	//     low:    { priority: 100,  preemptible: true }   # Can be preempted
	//     medium: { priority: 500,  preemptible: true }   # Can be preempted, preempts "low"
	//     high:   { priority: 1000, preemptible: false }  # Protected from fair-share preemption
	PriorityClasses map[string]types.PriorityClass `validate:"dive"`
	// DefaultPriorityClassName is assigned to jobs submitted without a priority class.
	// Must match a key in PriorityClasses.
	DefaultPriorityClassName string
	// PriorityClassNameOverride sets the Kubernetes PriorityClass name on pods sent to executors.
	// This controls what Kubernetes PriorityClass appears on the pod spec, not Armada's preemption behavior.
	//
	//   - "" (empty): Pods have no priorityClassName field
	//   - Any other value: All pods use this Kubernetes PriorityClass
	//
	// This is only relevant for clusters that require pods to have a Kubernetes PriorityClass set.
	// It does not affect how Armada schedules or preempts jobs.
	PriorityClassNameOverride *string
	// Number of jobs to load from the database at a time.
	MaxQueueLookback uint
	// In each invocation of the scheduler, no more jobs are scheduled once this limit has been exceeded.
	// Note that the total scheduled resources may be greater than this limit.
	MaximumResourceFractionToSchedule map[string]float64
	// Overrides MaximalClusterFractionToSchedule if set for the current pool.
	MaximumResourceFractionToScheduleByPool map[string]map[string]float64
	// The rate at which Armada schedules jobs is rate-limited using a token bucket approach.
	// Specifically, there is a token bucket that persists between scheduling rounds.
	// The bucket fills up at a rate of MaximumSchedulingRate tokens per second and has capacity MaximumSchedulingBurst.
	// A token is removed from the bucket when a scheduling a job and scheduling stops while the bucket is empty.
	//
	// Hence, MaximumSchedulingRate controls the maximum number of jobs scheduled per second in steady-state,
	// i.e., once the burst capacity has been exhausted.
	//
	// Rate-limiting is based on the number of tokens available at the start of each scheduling round,
	// i.e., tokens accumulated while scheduling become available at the start of the next scheduling round.
	//
	// For more information about the rate-limiter, see:
	// https://pkg.go.dev/golang.org/x/time/rate#Limiter
	MaximumSchedulingRate float64 `validate:"gt=0"`
	// MaximumSchedulingBurst controls the burst capacity of the rate-limiter.
	//
	// There are two important implications:
	// - Armada will never schedule more than MaximumSchedulingBurst jobs per scheduling round.
	// - Gang jobs with cardinality greater than MaximumSchedulingBurst can never be scheduled.
	MaximumSchedulingBurst int `validate:"gt=0"`
	// In addition to the global rate-limiter, there is a separate rate-limiter for each queue.
	// These work the same as the global rate-limiter, except they apply only to jobs scheduled from a specific queue.
	//
	// Per-queue version of MaximumSchedulingRate.
	MaximumPerQueueSchedulingRate float64 `validate:"gt=0"`
	// Per-queue version of MaximumSchedulingBurst.
	MaximumPerQueueSchedulingBurst int `validate:"gt=0"`
	// Maximum number of times a job is retried before considered failed.
	MaxRetries uint
	// List of resource names, e.g., []string{"cpu", "memory"}, to consider when computing DominantResourceFairness costs.
	// Dominant resource fairness is the algorithm used to assign a cost to jobs and queues.
	DominantResourceFairnessResourcesToConsider []string
	// Experimental - subject to change
	// List of resource names, (e.g. "cpu" or "memory"), to consider when computing DominantResourceFairness costs.
	// Dominant resource fairness is the algorithm used to assign a cost to jobs and queues.
	ExperimentalDominantResourceFairnessResourcesToConsider []DominantResourceFairnessResource
	// FairnessResources
	// Resource types (e.g. memory or nvidia.com/gpu) that the scheduler keeps track of.
	// Resource types not on this list will be ignored if seen on a node, and any jobs requesting them will fail.
	SupportedResourceTypes []ResourceType
	// Resources, e.g., "cpu", "memory", and "nvidia.com/gpu", for which the scheduler creates indexes for efficient lookup.
	// This list must contain at least one resource. Adding more than one resource is not required, but may speed up scheduling.
	// Ideally, this list contains all resources that frequently constrain which nodes a job can be scheduled onto.
	//
	// In particular, the allocatable resources on each node are rounded to a multiple of the resolution.
	// Lower resolution speeds up scheduling by improving node lookup speed but may prevent scheduling jobs,
	// since the allocatable resources may be rounded down to be a multiple of the resolution.
	//
	// See NodeDb docs for more details.
	IndexedResources []ResourceType
	// Node labels that the scheduler creates indexes for efficient lookup of.
	// Should include node labels frequently used by node selectors on submitted jobs.
	//
	// If not set, no labels are indexed.
	IndexedNodeLabels []string
	// Taint keys that the scheduler creates indexes for efficient lookup of.
	// Should include keys of taints frequently used in tolerations on submitted jobs.
	//
	// If not set, all taints are indexed.
	IndexedTaints []string
	// Experimental - subject to change
	// Resources that are outside of k8s, and not tied to a given k8s node or cluster.
	// For example connections to an S3 server that sits outside of k8s could be rationed to limit load on the server.
	// These can be requested like a normal k8s resource. Note there is no mechanism in armada
	// to enforce actual usage, it relies on honesty. For example, there is nothing to stop a badly-behaved job
	// requesting 2 S3 server connections and then opening 10.
	FloatingResources []FloatingResourceConfig
	// WellKnownNodeTypes defines a set of well-known node types used to define "home" and "away" nodes for a given priority class.
	WellKnownNodeTypes []WellKnownNodeType `validate:"dive"`
	// Executors that haven't heartbeated in this time period are considered stale.
	// No new jobs are scheduled onto stale executors.
	ExecutorTimeout time.Duration
	// Maximum number of jobs that can be assigned to a executor but not yet acknowledged, before
	// the scheduler is excluded from consideration by the scheduler.
	MaxUnacknowledgedJobsPerExecutor uint
	// The frequency at which the scheduler updates the cluster state.
	ExecutorUpdateFrequency time.Duration
	// Default priority for pools that are not in the above list
	DefaultPoolSchedulePriority int
	Pools                       []PoolConfig
	ExperimentalIndicativeShare ExperimentalIndicativeShare
}

const (
	DuplicateWellKnownNodeTypeErrorMessage     = "duplicate well-known node type name"
	AwayNodeTypesWithoutPreemptionErrorMessage = "priority class has away node types but is not preemptible"
	UnknownWellKnownNodeTypeErrorMessage       = "priority class refers to unknown well-known node type"
	InvalidSchedulingTimeoutErrorMessage       = "NewJobsSchedulingTimeout must be less than MaxSchedulingDuration"
	WildCardWellKnownNodeTypeValue             = "*"
)

// ResourceType represents a resource the scheduler indexes for efficient lookup.
type ResourceType struct {
	// Resource name, e.g., "cpu", "memory", or "nvidia.com/gpu".
	Name string
	// Resolution with which Armada tracks this resource; larger values indicate lower resolution.
	Resolution resource.Quantity
}

// DominantResourceFairnessResource - config for dominant resource fairness costs, the algorithm
// used to assign a cost to jobs and queues.
type DominantResourceFairnessResource struct {
	// Name of the resource type. For example, "cpu", "memory", or "nvidia.com/gpu".
	Name string
	// If set, Armada multiplies the cost for this resource by this number. If not set defaults to 1.
	Multiplier float64
}

// A WellKnownNodeType defines a set of nodes; see AwayNodeType.
type WellKnownNodeType struct {
	// Name is the unique identifier for this node type.
	Name string `validate:"required"`
	// Taints is the set of taints that characterizes this node type; a node is
	// part of this node type if and only if it has all of these taints.
	Taints []v1.Taint
}

type PoolConfig struct {
	Name                         string `validate:"required"`
	AwayPools                    []string
	ProtectedFractionOfFairShare *float64
	// List of resource names, (e.g. "cpu" or "memory"), to consider when computing DominantResourceFairness costs.
	// Dominant resource fairness is the algorithm used to assign a cost to jobs and queues.
	DominantResourceFairnessResourcesToConsider  []DominantResourceFairnessResource
	ExperimentalProtectUncappedAdjustedFairShare bool
	ExperimentalOptimiser                        *OptimiserConfig
	// When calculating costs assume all jobs ran for at least this long.
	// This penalizes jobs that ran for less than this value,
	// since they are charged the same as a job that ran for this value.
	ShortJobPenaltyCutoff         time.Duration
	ExperimentalSubmissionGroup   string
	ExperimentalMarketScheduling  *MarketSchedulingConfig
	ExperimentalRunReconciliation *RunReconciliationConfig
	DisableHomeScheduling         bool
	DisableAwayScheduling         bool
	DisableGangAwayScheduling     bool
}

func (p PoolConfig) GetSubmissionGroup() string {
	if p.ExperimentalSubmissionGroup == "" {
		return p.Name
	}
	return p.ExperimentalSubmissionGroup
}

type MarketSchedulingConfig struct {
	Enabled bool
	// The percentage of the pool that needs to be allocated to determine the spot price
	SpotPriceCutoff              float64
	GangIndicativePricingTimeout time.Duration `validate:"required"`
	// Set of gang definitions that need to be priced. Price will be exposed via metrics.
	GangsToPrice map[string]GangDefinition
}

type GangDefinition struct {
	Size              int32
	PriorityClassName string
	NodeUniformity    string // metadata.gresearch.co.uk/armada-gang-boundary
	NodeSelector      map[string]string
	Tolerations       []v1.Toleration
	Resources         *armadaresource.ComputeResources
}

type RunReconciliationConfig struct {
	Enabled                       bool
	EnsureReservationMatch        bool
	EnsureReservationDoesNotMatch bool
}

type OptimiserConfig struct {
	Enabled bool
	// How often the optimiser should run, likely desirable to not run every scheduling round
	Interval time.Duration
	// How long the optimiser can run for before giving up
	// The optimiser is relatively inefficient,
	//  on large pools this protects against the optimiser causing very long scheduling rounds
	Timeout time.Duration `validate:"required"`
	// Maximum jobs the optimiser will scheduler per round
	MaximumJobsPerRound int
	// Maximum fraction of the pool the optimiser will scheduler per round
	MaximumResourceFractionToSchedule map[string]float64
	// MinimumJobSizeToSchedule - The optimiser will not scheduler jobs that aren't at least as big as this field
	MinimumJobSizeToSchedule *armadaresource.ComputeResources
	// MaximumJobSizeToPreempt - The optimiser won't preempt jobs that are bigger than this field
	MaximumJobSizeToPreempt *armadaresource.ComputeResources
	// The minimum fairness improvement (as a percentage) for the optimiser to take action
	// I.e, Optimiser tries to scheduler a 16 CPU job and has to preempt a 10 CPU jobs
	// - 16/10 = 160%, 60% improvement
	MinimumFairnessImprovementPercentage float64
}

func (sc *SchedulingConfig) GetProtectedFractionOfFairShare(poolName string) float64 {
	for _, poolConfig := range sc.Pools {
		if poolConfig.Name == poolName && poolConfig.ProtectedFractionOfFairShare != nil {
			return *poolConfig.ProtectedFractionOfFairShare
		}
	}
	return sc.ProtectedFractionOfFairShare
}

func (sc *SchedulingConfig) GetProtectUncappedAdjustedFairShare(poolName string) bool {
	for _, poolConfig := range sc.Pools {
		if poolConfig.Name == poolName {
			return poolConfig.ExperimentalProtectUncappedAdjustedFairShare
		}
	}
	return false
}

func (sc *SchedulingConfig) GetOptimiserConfig(poolName string) *OptimiserConfig {
	for _, poolConfig := range sc.Pools {
		if poolConfig.Name == poolName {
			return poolConfig.ExperimentalOptimiser
		}
	}
	return nil
}

func (sc *SchedulingConfig) GetMarketConfig(poolName string) *MarketSchedulingConfig {
	for _, poolConfig := range sc.Pools {
		if poolConfig.Name == poolName {
			return poolConfig.ExperimentalMarketScheduling
		}
	}
	return nil
}

func (sc *SchedulingConfig) GetShortJobPenaltyCutoffs() map[string]time.Duration {
	result := make(map[string]time.Duration)
	for _, poolConfig := range sc.Pools {
		result[poolConfig.Name] = poolConfig.ShortJobPenaltyCutoff
	}
	return result
}

func (sc *SchedulingConfig) GetPoolConfig(poolName string) *PoolConfig {
	for _, poolConfig := range sc.Pools {
		if poolConfig.Name == poolName {
			return &poolConfig
		}
	}
	return nil
}

type ExperimentalIndicativeShare struct {
	BasePriorities []int
}

type PriorityOverrideConfig struct {
	Enabled         bool
	UpdateFrequency time.Duration
	ServiceUrl      string
	ForceNoTls      bool
}

type PricingApiConfig struct {
	Enabled         bool
	UpdateFrequency time.Duration
	ServiceUrl      string
	ForceNoTls      bool
	// This is for local testing only
	// It will stub the pricing api so it returns non-zero values but won't call and external service
	DevModeEnabled bool
}
