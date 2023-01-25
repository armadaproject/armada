package configuration

import (
	"time"

	"github.com/go-redis/redis"
	v1 "k8s.io/api/core/v1"

	authconfig "github.com/armadaproject/armada/internal/common/auth/configuration"
	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
)

const (
	// If set on a pod, the value of this annotation is interpreted as the id of a node
	// and only the node with that id will be considered for scheduling the pod.
	TargetNodeIdAnnotation = "armadaproject.io/targetNodeId"
)

type ArmadaConfig struct {
	Auth authconfig.AuthConfig

	GrpcPort    uint16
	HttpPort    uint16
	MetricsPort uint16

	CorsAllowedOrigins []string

	Grpc grpcconfig.GrpcConfig

	PriorityHalfTime    time.Duration
	CancelJobsBatchSize int
	Redis               redis.UniversalOptions
	EventsApiRedis      redis.UniversalOptions
	Scheduling          SchedulingConfig
	NewScheduler        NewSchedulerConfig
	QueueManagement     QueueManagementConfig
	DatabaseRetention   DatabaseRetentionPolicy
	Pulsar              PulsarConfig
	Postgres            PostgresConfig // Used for Pulsar submit API deduplication
	EventApi            EventApiConfig
	Metrics             MetricsConfig
}

type PulsarConfig struct {
	// Pulsar URL
	URL string
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
	CompressionType string
	// Compression Level to use.  Valid values are "Default", "Better", "Faster".  Default is "Default"
	CompressionLevel string
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
	Preemption                                PreemptionConfig
	UseProbabilisticSchedulingForAllResources bool
	// Number of jobs to load from the database at a time.
	QueueLeaseBatchSize uint
	// Minimum resources to schedule per request from an executor.
	// Applies to the old scheduler.
	MinimumResourceToSchedule armadaresource.ComputeResourcesFloat
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
	// The scheduler stores reports about scheduling decisions for each queue.
	// These can be queried by users. To limit memory usage, old reports are deleted
	// to keep the number of stored reports within this limit.
	MaxQueueReportsToStore int
	// The scheduler stores reports about scheduling decisions for each job.
	// These can be queried by users. To limit memory usage, old reports are deleted
	// to keep the number of stored reports within this limit.
	MaxJobReportsToStore int
	Lease                LeaseSettings
	DefaultJobLimits     armadaresource.ComputeResources
	// Set of tolerations added to all submitted pods.
	DefaultJobTolerations []v1.Toleration
	// Set of tolerations added to all submitted pods of a given priority class.
	DefaultJobTolerationsByPriorityClass map[string][]v1.Toleration
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
	// Jobs with equal value for this annotation make up a gang.
	// All jobs in a gang are guaranteed to be scheduled onto the same cluster at the same time.
	GangIdAnnotation string
	// All jobs in a gang must specify the total number of jobs in the gang via this annotation.
	// The cardinality should be expressed as an integer, e.g., "3".
	GangCardinalityAnnotation string
	// If an executor hasn't heartbeated in this time period, it will be considered stale
	ExecutorTimeout time.Duration
	// Label used to uniquely identify each node managed by Armada.
	// This label must be set on all nodes and its value must be unique across all nodes.
	NodeIdLabel string
}

// NewSchedulerConfig stores config for the new Pulsar-based scheduler.
// This scheduler will eventually replace the current scheduler.
type NewSchedulerConfig struct {
	Enabled bool
}

// TODO: Remove. Move PriorityClasses and DefaultPriorityClass into SchedulingConfig.
type PreemptionConfig struct {
	// If true, Armada will:
	// 1. Validate that submitted pods specify no or a valid priority class.
	// 2. Assign a default priority class to submitted pods that do not specify a priority class.
	// 3. Assign jobs to executors that may preempt currently running jobs.
	Enabled bool
	// Map from priority class names to priority classes.
	// Must be consistent with Kubernetes priority classes.
	// I.e., priority classes defined here must be defined in all executor clusters and should map to the same priority.
	PriorityClasses map[string]PriorityClass
	// Priority class assigned to pods that do not specify one.
	// Must be an entry in PriorityClasses above.
	DefaultPriorityClass string
}

type PriorityClass struct {
	Priority int32
	// Max fraction of resources assigned to jobs of this priority or lower.
	// Must be non-increasing with higher priority.
	//
	// For example, the following examples are valid configurations.
	// A:
	// - 2: 10%
	// - 1: 100%
	//
	// B:
	// - 9: 10%
	// - 5: 50%
	// - 3: 80%
	MaximalResourceFractionPerQueue map[string]float64
}

type DatabaseRetentionPolicy struct {
	JobRetentionDuration time.Duration
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
