package configuration

import (
	"time"

	"github.com/go-redis/redis"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common"
	authconfig "github.com/G-Research/armada/internal/common/auth/configuration"
	grpcconfig "github.com/G-Research/armada/internal/common/grpc/configuration"
	"github.com/G-Research/armada/pkg/client/queue"
)

type ArmadaConfig struct {
	Auth authconfig.AuthConfig

	GrpcPort    uint16
	HttpPort    uint16
	MetricsPort uint16

	CorsAllowedOrigins []string

	Grpc grpcconfig.GrpcConfig

	PriorityHalfTime      time.Duration
	CancelJobsBatchSize   int
	Redis                 redis.UniversalOptions
	Events                EventsConfig
	EventsNats            NatsConfig
	EventsJetstream       JetstreamConfig
	EventsRedis           redis.UniversalOptions
	EventsApiRedis        redis.UniversalOptions
	DefaultToLegacyEvents bool

	Scheduling        SchedulingConfig
	NewScheduler      NewSchedulerConfig
	QueueManagement   QueueManagementConfig
	DatabaseRetention DatabaseRetentionPolicy
	EventRetention    EventRetentionPolicy
	Pulsar            PulsarConfig
	Postgres          PostgresConfig // Used for Pulsar submit API deduplication
	EventApi          EventApiConfig
	Metrics           MetricsConfig
}

type PulsarConfig struct {
	// Flag controlling if Pulsar is enabled or not.
	Enabled bool
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
}

type SchedulingConfig struct {
	Preemption                                PreemptionConfig
	UseProbabilisticSchedulingForAllResources bool
	// Number of jobs to load from the database at a time.
	QueueLeaseBatchSize uint
	// Minimum resources to schedule per request from an executor.
	// Applies to the old scheduler.
	MinimumResourceToSchedule common.ComputeResourcesFloat
	// Maximum total size in bytes of all jobs returned in a single lease jobs call.
	// Applies to the old scheduler. But is not necessary since we now stream job leases.
	MaximumLeasePayloadSizeBytes int
	// Fraction of resources that can be assigned in a single lease jobs call.
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
	MaximumJobsToSchedule int
	// Probability of using the new sheduler.
	// Set to 0 to disable the new scheduler and to 1 to disable the old scheduler.
	ProbabilityOfUsingNewScheduler float64
	// The scheduler stores reports about scheduling decisions for each queue.
	// These can be queried by users. To limit memory usage, old reports are deleted
	// to keep the number of stored reports within this limit.
	MaxQueueReportsToStore int
	// The scheduler stores reports about scheduling decisions for each job.
	// These can be queried by users. To limit memory usage, old reports are deleted
	// to keep the number of stored reports within this limit.
	MaxJobReportsToStore  int
	Lease                 LeaseSettings
	DefaultJobLimits      common.ComputeResources
	DefaultJobTolerations []v1.Toleration
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
	IndexedNodeLabels map[string]interface{}
	// Taint keys that the scheduler creates indexes for efficient lookup of.
	// Should include taints frequently used for scheduling.
	// Since the scheduler can efficiently sort out nodes for which these taints
	// are not set correctly when looking for a node a pod can be scheduled on.
	//
	// If not set, all taints are indexed.
	//
	// Applies only to the new scheduler.
	IndexedTaints                 map[string]interface{}
	MinTerminationGracePeriod     time.Duration
	MaxTerminationGracePeriod     time.Duration
	DefaultTerminationGracePeriod time.Duration
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
	// Map from priority class name to priority.
	// Must be consistent with Kubernetes priority classes.
	// I.e., priority classes defined here must be defined in all executor clusters and should map to the same priority.
	PriorityClasses map[string]int32
	// Priority class assigned to pods that do not specify one.
	// Must be an entry in PriorityClasses above.
	DefaultPriorityClass string
}

type DatabaseRetentionPolicy struct {
	JobRetentionDuration time.Duration
}

type EventRetentionPolicy struct {
	ExpiryEnabled     bool
	RetentionDuration time.Duration
}

type LeaseSettings struct {
	ExpireAfter        time.Duration
	ExpiryLoopInterval time.Duration
}

type EventsConfig struct {
	StoreQueue     string // Queue group for event storage processors
	JobStatusQueue string // Queue group for running job status processor

	ProcessorBatchSize             int           // Maximum event batch size
	ProcessorMaxTimeBetweenBatches time.Duration // Maximum time between batches
	ProcessorTimeout               time.Duration // Timeout for reporting event or stopping batcher before erroring out
}

type PostgresConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	Connection      map[string]string
}

type NatsConfig struct {
	Servers   []string
	ClusterID string
	Subject   string
	Timeout   time.Duration // Timeout for receiving a reply back from the stan server for PublishAsync
}

type JetstreamConfig struct {
	Servers     []string
	StreamName  string
	Replicas    int
	Subject     string
	MaxAgeDays  int
	ConnTimeout time.Duration
	InMemory    bool // Whether stream should be stored in memory (as opposed to on disk)
}

type QueueManagementConfig struct {
	AutoCreateQueues       bool
	DefaultPriorityFactor  queue.PriorityFactor
	DefaultQueuedJobsLimit int
}

type MetricsConfig struct {
	RefreshInterval time.Duration
}

type EventApiConfig struct {
	Enabled          bool
	QueryConcurrency int
	JobsetCacheSize  int
	UpdateTopic      string
	Postgres         PostgresConfig
}
