package configuration

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-redis/redis"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"

	authconfig "github.com/armadaproject/armada/internal/common/auth/configuration"
	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
)

type ArmadaConfig struct {
	Auth authconfig.AuthConfig

	GrpcPort    uint16
	HttpPort    uint16
	MetricsPort uint16

	CorsAllowedOrigins []string

	Grpc grpcconfig.GrpcConfig

	PriorityHalfTime                  time.Duration
	CancelJobsBatchSize               int
	Redis                             redis.UniversalOptions
	EventsApiRedis                    redis.UniversalOptions
	Scheduling                        SchedulingConfig
	NewScheduler                      NewSchedulerConfig
	QueueManagement                   QueueManagementConfig
	Pulsar                            PulsarConfig
	Postgres                          PostgresConfig // Used for Pulsar submit API deduplication
	EventApi                          EventApiConfig
	Metrics                           MetricsConfig
	IgnoreJobSubmitChecks             bool // Temporary flag to stop us rejecting jobs on switch over
	PulsarSchedulerEnabled            bool
	ProbabilityOfUsingPulsarScheduler float64
}

type PulsarConfig struct {
	// Pulsar URL
	URL string `validate:"required"`
	// Path to the trusted TLS certificate file (must exist)
	TLSTrustCertsFilePath string
	// Whether Pulsar client accept untrusted TLS certificate from broker
	TLSAllowInsecureConnection bool
	// Whether the Pulsar client will validate the hostname in the broker's TLS Cert matches the actual hostname.
	TLSValidateHostname bool
	// Max number of connections to a single broker that will be kept in the pool. (Default: 1 connection)
	MaxConnectionsPerBroker int
	// Whether Pulsar authentication is enabled
	AuthenticationEnabled bool
	// Authentication type. For now only "JWT" auth is valid
	AuthenticationType string
	// Path to the JWT token (must exist). This must be set if AutheticationType is "JWT"
	JwtTokenPath                string
	JobsetEventsTopic           string
	RedisFromPulsarSubscription string
	// Compression to use.  Valid values are "None", "LZ4", "Zlib", "Zstd".  Default is "None"
	CompressionType pulsar.CompressionType
	// Compression Level to use.  Valid values are "Default", "Better", "Faster".  Default is "Default"
	CompressionLevel pulsar.CompressionLevel
	// Used to construct an executorconfig.IngressConfiguration,
	// which is used when converting Armada-specific IngressConfig and ServiceConfig objects into k8s objects.
	HostnameSuffix string
	CertNameSuffix string
	Annotations    map[string]string
	// Settings for deduplication, which relies on a postgres server.
	DedupTable string
	// Log all pulsar events
	EventsPrinterSubscription string
	EventsPrinter             bool
	// Maximum allowed message size in bytes
	MaxAllowedMessageSize uint
	// Timeout when polling pulsar for messages
	ReceiveTimeout time.Duration
	// Backoff from polling when Pulsar returns an error
	BackoffTime time.Duration
}

type SchedulingConfig struct {
	// Set to true to enable scheduler assertions. This results in some performance loss.
	EnableAssertions bool
	Preemption       PreemptionConfig
	// Number of jobs to load from the database at a time.
	QueueLeaseBatchSize uint
	// Maximum total size in bytes of all jobs returned in a single lease jobs call.
	// Applies to the old scheduler. But is not necessary since we now stream job leases.
	MaximumLeasePayloadSizeBytes int
	// Fraction of total resources across clusters that can be assigned in a single lease jobs call.
	// Applies to both the old and new scheduler.
	MaximalClusterFractionToSchedule map[string]float64
	// Fraction of resources that can be assigned to any single queue,
	// within a single lease jobs call.
	// Applies to both the old and new scheduler.
	MaximalResourceFractionToSchedulePerQueue map[string]float64
	// Fraction of resources that can be assigned to any single queue.
	// Applies to both the old and new scheduler.
	MaximalResourceFractionPerQueue map[string]float64
	// Max number of jobs to scheduler per lease jobs call.
	MaximumJobsToSchedule uint
	// Max number of gangs to scheduler per lease jobs call.
	MaximumGangsToSchedule uint
	// Armada stores contexts associated with recent job scheduling attempts.
	// This setting limits the number of such contexts to store.
	// Contexts associated with the most recent scheduling attempt for each queue and cluster are always stored.
	MaxJobSchedulingContextsPerExecutor uint
	Lease                               LeaseSettings
	DefaultJobLimits                    armadaresource.ComputeResources
	// Set of tolerations added to all submitted pods.
	DefaultJobTolerations []v1.Toleration
	// Set of tolerations added to all submitted pods of a given priority class.
	DefaultJobTolerationsByPriorityClass map[string][]v1.Toleration
	// Set of tolerations added to all submitted pods with a given resource request.
	DefaultJobTolerationsByResourceRequest map[string][]v1.Toleration
	// Maximum number of times a job is retried before considered failed.
	MaxRetries uint
	// Weights used when computing fair share.
	// Overrides dynamic scarcity calculation if provided.
	// Applies to both the new and old scheduler.
	ResourceScarcity map[string]float64
	// Applies only to the old scheduler.
	PoolResourceScarcity map[string]map[string]float64
	MaxPodSpecSizeBytes  uint
	MinJobResources      v1.ResourceList
	// Once a node has been found on which a pod can be scheduled,
	// the scheduler will consider up to the next maxExtraNodesToConsider nodes.
	// The scheduler selects the node with the best score out of the considered nodes.
	// In particular, the score expresses whether preemption is necessary to schedule a pod.
	// Hence, a larger MaxExtraNodesToConsider would reduce the expected number of preemptions.
	MaxExtraNodesToConsider uint
	// Resources, e.g., "cpu", "memory", and "nvidia.com/gpu",
	// for which the scheduler creates indexes for efficient lookup.
	// Applies only to the new scheduler.
	IndexedResources []string
	// Node labels that the scheduler creates indexes for efficient lookup of.
	// Should include node labels frequently used for scheduling.
	// Since the scheduler can efficiently sort out nodes for which these labels
	// are not set correctly when looking for a node a pod can be scheduled on.
	//
	// If not set, no labels are indexed.
	//
	// Applies only to the new scheduler.
	IndexedNodeLabels []string
	// Taint keys that the scheduler creates indexes for efficient lookup of.
	// Should include taints frequently used for scheduling.
	// Since the scheduler can efficiently sort out nodes for which these taints
	// are not set correctly when looking for a node a pod can be scheduled on.
	//
	// If not set, all taints are indexed.
	//
	// Applies only to the new scheduler.
	IndexedTaints []string
	// Kubernetes pods may specify a termination grace period.
	// When Pods are cancelled/preempted etc., they are first sent a SIGTERM.
	// If a pod has not exited within its termination grace period,
	// it is killed forcefully by Kubernetes sending it a SIGKILL.
	//
	// This is the minimum allowed termination grace period.
	// It should normally be set to a positive value, e.g., 1 second.
	// Since a zero grace period causes Kubernetes to force delete pods,
	// which may causes issues where resources associated with the pod, e.g.,
	// containers, are not cleaned up correctly.
	//
	// The grace period of pods that either
	// - do not set a grace period, or
	// - explicitly set a grace period of 0 seconds,
	// is automatically set to MinTerminationGracePeriod.
	MinTerminationGracePeriod time.Duration
	// Max allowed grace period.
	// Should normally not be set greater than single-digit minutes,
	// since cancellation and preemption may need to wait for this amount of time.
	MaxTerminationGracePeriod time.Duration
	// If an executor hasn't heartbeated in this time period, it will be considered stale
	ExecutorTimeout time.Duration
	// Default activeDeadline for all pods that don't explicitly set activeDeadlineSeconds.
	// Is trumped by DefaultActiveDeadlineByResourceRequest.
	DefaultActiveDeadline time.Duration
	// Default activeDeadline for pods with at least one container requesting a given resource.
	// For example, if
	// DefaultActiveDeadlineByResourceRequest: map[string]time.Duration{"gpu": time.Second},
	// then all pods requesting a non-zero amount of gpu and don't explicitly set activeDeadlineSeconds
	// will have activeDeadlineSeconds set to 1. Trumps DefaultActiveDeadline.
	DefaultActiveDeadlineByResourceRequest map[string]time.Duration
}

// NewSchedulerConfig stores config for the new Pulsar-based scheduler.
// This scheduler will eventually replace the current scheduler.
type NewSchedulerConfig struct {
	Enabled bool
}

// TODO: Remove. Move PriorityClasses and DefaultPriorityClass into SchedulingConfig.
type PreemptionConfig struct {
	// If using PreemptToFairShare,
	// the probability of evicting jobs on a node to balance resource usage.
	NodeEvictionProbability float64
	// If using PreemptToFairShare,
	// the probability of evicting jobs on oversubscribed nodes, i.e.,
	// nodes on which the total resource requests are greater than the available resources.
	NodeOversubscriptionEvictionProbability float64
	// If true, the Armada scheduler will add to scheduled pods a node selector
	// NodeIdLabel: <value of label on node selected by scheduler>.
	// If true, NodeIdLabel must be non-empty.
	SetNodeIdSelector bool
	// Label used with SetNodeIdSelector. Must be non-empty if SetNodeIdSelector is true.
	NodeIdLabel string
	// If true, the Armada scheduler will set the node name of the selected node directly on scheduled pods,
	// thus bypassing kube-scheduler entirely.
	SetNodeName bool
	// Map from priority class names to priority classes.
	// Must be consistent with Kubernetes priority classes.
	// I.e., priority classes defined here must be defined in all executor clusters and should map to the same priority.
	PriorityClasses map[string]PriorityClass
	// Priority class assigned to pods that do not specify one.
	// Must be an entry in PriorityClasses above.
	DefaultPriorityClass string
	// If set, override the priority class name of pods with this value when sending to an executor.
	PriorityClassNameOverride *string
}

type PriorityClass struct {
	Priority int32
	// If true, Armada may preempt jobs of this class to improve fairness.
	Preemptible bool
	// Limits resources assigned to jobs of priority equal to or lower than that of this priority class.
	// Specifically, jobs of this priority class are only scheduled if doing so does not exceed this limit.
	//
	// For example, if priority is 10 and MaximalResourceFractionPerQueue is map[string]float64{"cpu": 0.3},
	// jobs of this priority class are not scheduled if doing so would cause the total resources assigned
	// to jobs of priority 10 or lower from the same queue to exceed 30% of the total.
	MaximalResourceFractionPerQueue map[string]float64
}

func (p PreemptionConfig) PriorityByPriorityClassName() map[string]int32 {
	return PriorityByPriorityClassName(p.PriorityClasses)
}

func PriorityByPriorityClassName(priorityClasses map[string]PriorityClass) map[string]int32 {
	rv := make(map[string]int32, len(priorityClasses))
	for name, pc := range priorityClasses {
		rv[name] = pc.Priority
	}
	return rv
}

func (p PreemptionConfig) AllowedPriorities() []int32 {
	return AllowedPriorities(p.PriorityClasses)
}

func AllowedPriorities(priorityClasses map[string]PriorityClass) []int32 {
	rv := make([]int32, 0, len(priorityClasses))
	for _, v := range priorityClasses {
		rv = append(rv, v.Priority)
	}
	slices.Sort(rv)
	return slices.Compact(rv)
}

type LeaseSettings struct {
	ExpireAfter        time.Duration
	ExpiryLoopInterval time.Duration
}

type PostgresConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	Connection      map[string]string
}

type QueueManagementConfig struct {
	AutoCreateQueues       bool
	DefaultPriorityFactor  float64
	DefaultQueuedJobsLimit int
}

type MetricsConfig struct {
	Port            uint16
	RefreshInterval time.Duration
}

type EventApiConfig struct {
	Enabled          bool
	QueryConcurrency int
	JobsetCacheSize  int
	UpdateTopic      string
	Postgres         PostgresConfig
}
