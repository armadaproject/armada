package configuration

import (
	"time"

	"github.com/go-redis/redis"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/auth/configuration"
	"github.com/G-Research/armada/pkg/client/queue"
)

type ArmadaConfig struct {
	Auth configuration.AuthConfig

	GrpcPort           uint16
	HttpPort           uint16
	MetricsPort        uint16
	CorsAllowedOrigins []string

	PriorityHalfTime    time.Duration
	CancelJobsBatchSize int
	Redis               redis.UniversalOptions
	Events              EventsConfig
	EventsNats          NatsConfig
	EventsJetstream     JetstreamConfig
	EventsRedis         redis.UniversalOptions

	Scheduling        SchedulingConfig
	QueueManagement   QueueManagementConfig
	DatabaseRetention DatabaseRetentionPolicy
	EventRetention    EventRetentionPolicy
	Pulsar            PulsarConfig

	Metrics MetricsConfig
}

type PulsarConfig struct {
	// Flag controlling if Pulsar is enabled or not.
	Enabled bool
	// Pulsar configuration
	URL string
	// Path to the trusted TLS certificate file (must exist)
	TLSTrustCertsFilePath string
	// Whether Pulsar client accept untrusted TLS certificate from broker
	TLSAllowInsecureConnection bool
	// Whether the Pulsar client will validate the hostname in the broker's TLS Cert matches the actual hostname.
	TLSValidateHostname bool
	// Max number of connections to a single broker that will be kept in the pool. (Default: 1 connection)
	MaxConnectionsPerBroker int
	// Whether Pulsar authetication is enabled
	AuthenticationEnabled bool
	// Authentication type. For now only "JWT" auth is valid
	AuthenticationType string
	// Path to the JWT token (must exist). This must be set if AutheticationType is "JWT"
	JwtTokenPath                 string
	JobsetEventsTopic            string
	RedisFromPulsarSubscription  string
	PulsarFromPulsarSubscription string
	// Compression to use.  Valid values are "None", "LZ4", "Zlib", "Zstd".  Default is "None"
	CompressionType string
	// Compression Level to use.  Valid values are "Default", "Better", "Faster".  Default is "Default"
	CompressionLevel string
	// Used to construct an executorconfig.IngressConfiguration,
	// which is used when converting Armada-specific IngressConfig and ServiceConfig objects into k8s objects.
	HostnameSuffix string
	CertNameSuffix string
	Annotations    map[string]string
}

type SchedulingConfig struct {
	UseProbabilisticSchedulingForAllResources bool
	QueueLeaseBatchSize                       uint
	MinimumResourceToSchedule                 common.ComputeResourcesFloat
	MaximumLeasePayloadSizeBytes              int
	MaximalClusterFractionToSchedule          map[string]float64
	MaximalResourceFractionToSchedulePerQueue map[string]float64
	MaximalResourceFractionPerQueue           map[string]float64
	MaximumJobsToSchedule                     int
	Lease                                     LeaseSettings
	DefaultJobLimits                          common.ComputeResources
	DefaultJobTolerations                     []v1.Toleration
	MaxRetries                                uint // Maximum number of retries before a Job is failed
	ResourceScarcity                          map[string]float64
	PoolResourceScarcity                      map[string]map[string]float64
	MaxPodSpecSizeBytes                       uint
	MinJobResources                           v1.ResourceList
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
	AutoCreateQueues      bool
	DefaultPriorityFactor queue.PriorityFactor
}

type MetricsConfig struct {
	RefreshInterval time.Duration
}
