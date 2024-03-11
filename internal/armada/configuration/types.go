package configuration

import (
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-playground/validator/v10"
	"github.com/go-redis/redis"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	authconfig "github.com/armadaproject/armada/internal/common/auth/configuration"
	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/common/types"
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

	CancelJobsBatchSize int
	Redis               redis.UniversalOptions
	EventsApiRedis      redis.UniversalOptions
	Scheduling          SchedulingConfig
	Pulsar              PulsarConfig
	Postgres            PostgresConfig // Used for Pulsar submit API deduplication
	QueryApi            QueryApiConfig
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
	// Log all pulsar events
	EventsPrinterSubscription string
	EventsPrinter             bool
	// Maximum allowed message size in bytes
	MaxAllowedMessageSize uint
	// Timeout when polling pulsar for messages
	ReceiveTimeout time.Duration
	// Backoff from polling when Pulsar returns an error
	BackoffTime time.Duration
	// Number of pulsar messages that will be queued by the pulsar consumer.
	ReceiverQueueSize int
}

type SchedulingConfig struct {
	// Set to true to disable scheduling
	DisableScheduling bool
	// Set to true to enable scheduler assertions. This results in some performance loss.
	EnableAssertions bool
	// If using PreemptToFairShare,
	// the probability of evicting jobs on a node to balance resource usage.
	// TODO(albin): Remove.
	NodeEvictionProbability float64
	// If using PreemptToFairShare,
	// the probability of evicting jobs on oversubscribed nodes, i.e.,
	// nodes on which the total resource requests are greater than the available resources.
	// TODO(albin): Remove.
	NodeOversubscriptionEvictionProbability float64
	// Only queues allocated more than this fraction of their fair share are considered for preemption.
	ProtectedFractionOfFairShare float64 `validate:"gte=0"`
	// Armada adds a node selector term to every scheduled pod using this label with the node name as value.
	// This to force kube-scheduler to schedule pods on the node chosen by Armada.
	// For example, if NodeIdLabel is "kubernetes.io/hostname" and armada schedules a pod on node "myNode",
	// then Armada adds "kubernetes.io/hostname": "myNode" to the pod node selector before sending it to the executor.
	NodeIdLabel string `validate:"required"`
	// Map from priority class names to priority classes.
	// Must be consistent with Kubernetes priority classes.
	// I.e., priority classes defined here must be defined in all executor clusters and should map to the same priority.
	PriorityClasses map[string]types.PriorityClass `validate:"dive"`
	// Priority class assigned to pods that do not specify one.
	// Must be an entry in PriorityClasses above.
	DefaultPriorityClass string
	// If set, override the priority class name of pods with this value when sending to an executor.
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
	// Armada stores contexts associated with recent job scheduling attempts.
	// This setting limits the number of such contexts to store.
	// Contexts associated with the most recent scheduling attempt for each queue and cluster are always stored.
	MaxJobSchedulingContextsPerExecutor uint
	DefaultJobLimits                    armadaresource.ComputeResources
	// Set of tolerations added to all submitted pods.
	DefaultJobTolerations []v1.Toleration
	// Set of tolerations added to all submitted pods of a given priority class.
	DefaultJobTolerationsByPriorityClass map[string][]v1.Toleration
	// Set of tolerations added to all submitted pods with a given resource request.
	DefaultJobTolerationsByResourceRequest map[string][]v1.Toleration
	// Maximum number of times a job is retried before considered failed.
	MaxRetries uint
	// List of resource names, e.g., []string{"cpu", "memory"}, to consider when computing DominantResourceFairness.
	DominantResourceFairnessResourcesToConsider []string
	MaxPodSpecSizeBytes                         uint
	MinJobResources                             v1.ResourceList
	// Once a node has been found on which a pod can be scheduled,
	// the scheduler will consider up to the next maxExtraNodesToConsider nodes.
	// The scheduler selects the node with the best score out of the considered nodes.
	// In particular, the score expresses whether preemption is necessary to schedule a pod.
	// Hence, a larger MaxExtraNodesToConsider would reduce the expected number of preemptions.
	MaxExtraNodesToConsider uint
	// Resources, e.g., "cpu", "memory", and "nvidia.com/gpu",
	// for which the scheduler creates indexes for efficient lookup.
	// Applies only to the new scheduler.
	IndexedResources []IndexedResource
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
	// WellKnownNodeTypes defines a set of well-known node types; these are used
	// to define "home" and "away" nodes for a given priority class.
	WellKnownNodeTypes []WellKnownNodeType `validate:"dive"`
	// Default value of GangNodeUniformityLabelAnnotation if none is provided.
	DefaultGangNodeUniformityLabel string
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
	// Maximum number of jobs that can be assigned to a executor but not yet acknowledged, before
	// the scheduler is excluded from consideration by the scheduler.
	MaxUnacknowledgedJobsPerExecutor uint
	// If true, do not during scheduling skip jobs with requirements known to be impossible to meet.
	AlwaysAttemptScheduling bool
	// The frequency at which the scheduler updates the cluster state.
	ExecutorUpdateFrequency time.Duration
	// Controls node and queue success probability estimation.
	FailureEstimatorConfig FailureEstimatorConfig
}

const (
	DuplicateWellKnownNodeTypeErrorMessage     = "duplicate well-known node type name"
	AwayNodeTypesWithoutPreemptionErrorMessage = "priority class has away node types but is not preemptible"
	UnknownWellKnownNodeTypeErrorMessage       = "priority class refers to unknown well-known node type"
)

func SchedulingConfigValidation(sl validator.StructLevel) {
	c := sl.Current().Interface().(SchedulingConfig)

	wellKnownNodeTypes := make(map[string]bool)
	for i, wellKnownNodeType := range c.WellKnownNodeTypes {
		if wellKnownNodeTypes[wellKnownNodeType.Name] {
			fieldName := fmt.Sprintf("WellKnownNodeTypes[%d].Name", i)
			sl.ReportError(wellKnownNodeType.Name, fieldName, "", DuplicateWellKnownNodeTypeErrorMessage, "")
		}
		wellKnownNodeTypes[wellKnownNodeType.Name] = true
	}

	for priorityClassName, priorityClass := range c.PriorityClasses {
		if len(priorityClass.AwayNodeTypes) > 0 && !priorityClass.Preemptible {
			fieldName := fmt.Sprintf("Preemption.PriorityClasses[%s].Preemptible", priorityClassName)
			sl.ReportError(priorityClass.Preemptible, fieldName, "", AwayNodeTypesWithoutPreemptionErrorMessage, "")
		}

		for i, awayNodeType := range priorityClass.AwayNodeTypes {
			if !wellKnownNodeTypes[awayNodeType.WellKnownNodeTypeName] {
				fieldName := fmt.Sprintf("Preemption.PriorityClasses[%s].AwayNodeTypes[%d].WellKnownNodeTypeName", priorityClassName, i)
				sl.ReportError(awayNodeType.WellKnownNodeTypeName, fieldName, "", UnknownWellKnownNodeTypeErrorMessage, "")
			}
		}
	}
}

type IndexedResource struct {
	// Resource name. E.g., "cpu", "memory", or "nvidia.com/gpu".
	Name string
	// See NodeDb docs.
	Resolution resource.Quantity
}

// A WellKnownNodeType defines a set of nodes; see AwayNodeType.
type WellKnownNodeType struct {
	// Name is the unique identifier for this node type.
	Name string `validate:"required"`
	// Taints is the set of taints that characterizes this node type; a node is
	// part of this node type if and only if it has all of these taints.
	Taints []v1.Taint
}

// FailureEstimatorConfig contains config controlling node and queue success probability estimation.
// See the internal/scheduler/failureestimator package for details.
type FailureEstimatorConfig struct {
	Disabled                           bool
	NumInnerIterations                 int     `validate:"gt=0"`
	InnerOptimiserStepSize             float64 `validate:"gt=0"`
	OuterOptimiserStepSize             float64 `validate:"gt=0"`
	OuterOptimiserNesterovAcceleration float64 `validate:"gte=0"`
}

// TODO: we can probably just typedef this to map[string]string
type PostgresConfig struct {
	Connection map[string]string
}

type QueryApiConfig struct {
	Enabled       bool
	Postgres      PostgresConfig
	MaxQueryItems int
}
