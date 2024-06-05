package configuration

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/redis/go-redis/v9"
	v1 "k8s.io/api/core/v1"

	authconfig "github.com/armadaproject/armada/internal/common/auth/configuration"
	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/pkg/client"
)

type ArmadaConfig struct {
	Auth authconfig.AuthConfig

	GrpcPort    uint16
	HttpPort    uint16
	MetricsPort uint16
	// If non-nil, net/http/pprof endpoints are exposed on localhost on this port.
	PprofPort *uint16

	CorsAllowedOrigins []string
	GrpcGatewayPath    string

	Grpc grpcconfig.GrpcConfig

	SchedulerApiConnection client.ApiConnectionDetails

	EventsApiRedis redis.UniversalOptions
	Pulsar         PulsarConfig
	Postgres       PostgresConfig // Needs to point to the lookout db
	QueryApi       QueryApiConfig

	// Period At which the Queue cache will be refreshed
	QueueCacheRefreshPeriod time.Duration

	// Config relating to job submission.
	Submission SubmissionConfig
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
	// Path to the JWT token (must exist). This must be set if AuthenticationType is "JWT"
	JwtTokenPath                string
	JobsetEventsTopic           string
	RedisFromPulsarSubscription string
	// Compression to use.  Valid values are "None", "LZ4", "Zlib", "Zstd".  Default is "None"
	CompressionType pulsar.CompressionType
	// Compression Level to use.  Valid values are "Default", "Better", "Faster".  Default is "Default"
	CompressionLevel pulsar.CompressionLevel
	// Settings for deduplication, which relies on a postgres server.
	DedupTable string
	// Maximum allowed message size in bytes
	MaxAllowedMessageSize uint
	// Timeout when polling pulsar for messages
	ReceiveTimeout time.Duration
	// Backoff from polling when Pulsar returns an error
	BackoffTime time.Duration
	// Number of pulsar messages that will be queued by the pulsar consumer.
	ReceiverQueueSize int
}

// SubmissionConfig contains config relating to job submission.
type SubmissionConfig struct {
	// The priorityClassName field on submitted pod must be either empty or in this list.
	// These names should correspond to priority classes defined in schedulingConfig.
	AllowedPriorityClassNames map[string]bool
	// Priority class name assigned to pods that do not specify one.
	// Must be an entry in PriorityClasses above.
	DefaultPriorityClassName string
	// Default job resource limits added to pods.
	DefaultJobLimits armadaresource.ComputeResources
	// Tolerations added to all submitted pods.
	DefaultJobTolerations []v1.Toleration
	// Tolerations added to all submitted pods of a given priority class.
	DefaultJobTolerationsByPriorityClass map[string][]v1.Toleration
	// Tolerations added to all submitted pods requesting a non-zero amount of some resource.
	DefaultJobTolerationsByResourceRequest map[string][]v1.Toleration
	// Pods of size greater than this are rejected at submission.
	MaxPodSpecSizeBytes uint
	// Jobs requesting less than this amount of resources are rejected at submission.
	MinJobResources v1.ResourceList
	// Default value of GangNodeUniformityLabelAnnotation if not set on submitted jobs.
	// TODO(albin): We should add a label to nodes in the nodeDb indicating which cluster it came from.
	//              If we do, we can default to that label if the uniformity label is empty.
	DefaultGangNodeUniformityLabel string
	// Minimum allowed termination grace period for pods submitted to Armada.
	// Should normally be set to a positive value, e.g., "10m".
	// Since a zero grace period causes Kubernetes to force delete pods, which may causes issues with container resource cleanup.
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
	// Default activeDeadline for all pods that don't explicitly set activeDeadlineSeconds.
	// Is trumped by DefaultActiveDeadlineByResourceRequest.
	DefaultActiveDeadline time.Duration
	// Default activeDeadline for pods with at least one container requesting a given resource.
	// For example, if
	// DefaultActiveDeadlineByResourceRequest: map[string]time.Duration{"gpu": time.Second},
	// then all pods requesting a non-zero amount of gpu and don't explicitly set activeDeadlineSeconds
	// will have activeDeadlineSeconds set to 1.
	// Trumps DefaultActiveDeadline.
	DefaultActiveDeadlineByResourceRequest map[string]time.Duration
}

// TODO: we can probably just typedef this to map[string]string
type PostgresConfig struct {
	Connection map[string]string
}

type QueryApiConfig struct {
	MaxQueryItems int
}
