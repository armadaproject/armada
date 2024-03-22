package configuration

import (
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-playground/validator/v10"
	"github.com/redis/go-redis/v9"
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

	Redis          redis.UniversalOptions
	EventsApiRedis redis.UniversalOptions
	Pulsar         PulsarConfig
	Postgres       PostgresConfig // Used for Pulsar submit API deduplication
	QueryApi       QueryApiConfig

	// Config relating to job submission.
	Submission SubmissionConfig
	// Scheduling config used by the submitChecker.
	Scheduling SchedulingConfig
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
	// Jobs with no priority class are assigned this priority class when ingested by the scheduler.
	// Must be a key in the PriorityClasses map above.
	DefaultPriorityClassName string
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
	// Maximum number of times a job is retried before considered failed.
	MaxRetries uint
	// List of resource names, e.g., []string{"cpu", "memory"}, to consider when computing DominantResourceFairness.
	DominantResourceFairnessResourcesToConsider []string
	// Once a node has been found on which a pod can be scheduled,
	// the scheduler will consider up to the next maxExtraNodesToConsider nodes.
	// The scheduler selects the node with the best score out of the considered nodes.
	// In particular, the score expresses whether preemption is necessary to schedule a pod.
	// Hence, a larger MaxExtraNodesToConsider would reduce the expected number of preemptions.
	// TODO(albin): Remove. It's unused.
	MaxExtraNodesToConsider uint
	// Resources, e.g., "cpu", "memory", and "nvidia.com/gpu", for which the scheduler creates indexes for efficient lookup.
	// This list must contain at least one resource. Adding more than one resource is not required, but may speed up scheduling.
	// Ideally, this list contains all resources that frequently constrain which nodes a job can be scheduled onto.
	IndexedResources []IndexedResource
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
	// WellKnownNodeTypes defines a set of well-known node types used to define "home" and "away" nodes for a given priority class.
	WellKnownNodeTypes []WellKnownNodeType `validate:"dive"`
	// Executor that haven't heartbeated in this time period are considered stale.
	// No new jobs are scheduled onto stale executors.
	ExecutorTimeout time.Duration
	// Maximum number of jobs that can be assigned to a executor but not yet acknowledged, before
	// the scheduler is excluded from consideration by the scheduler.
	MaxUnacknowledgedJobsPerExecutor uint
	// If true, do not during scheduling skip jobs with requirements known to be impossible to meet.
	AlwaysAttemptScheduling bool
	// The frequency at which the scheduler updates the cluster state.
	ExecutorUpdateFrequency time.Duration
	// Controls node and queue success probability estimation.
	FailureProbabilityEstimation FailureEstimatorConfig
	// Controls node quarantining, i.e., removing from consideration for scheduling misbehaving nodes.
	NodeQuarantining NodeQuarantinerConfig
	// Controls queue quarantining, i.e., rate-limiting scheduling from misbehaving queues.
	QueueQuarantining QueueQuarantinerConfig
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

// IndexedResource represents a resource the scheduler indexes for efficient lookup.
type IndexedResource struct {
	// Resource name, e.g., "cpu", "memory", or "nvidia.com/gpu".
	Name string
	// Resolution with which Armada tracks this resource; larger values indicate lower resolution.
	// In particular, the allocatable resources on each node are rounded to a multiple of the resolution.
	// Lower resolution speeds up scheduling by improving node lookup speed but may prevent scheduling jobs,
	// since the allocatable resources may be rounded down to be a multiple of the resolution.
	//
	// See NodeDb docs for more details.
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

// FailureEstimatorConfig controls node and queue success probability estimation.
// See internal/scheduler/failureestimator.go for details.
type FailureEstimatorConfig struct {
	Disabled                           bool
	NumInnerIterations                 int     `validate:"gt=0"`
	InnerOptimiserStepSize             float64 `validate:"gt=0"`
	OuterOptimiserStepSize             float64 `validate:"gt=0"`
	OuterOptimiserNesterovAcceleration float64 `validate:"gte=0"`
}

// NodeQuarantinerConfig controls how nodes are quarantined, i.e., removed from consideration when scheduling new jobs.
// See internal/scheduler/quarantine/node_quarantiner.go for details.
type NodeQuarantinerConfig struct {
	FailureProbabilityQuarantineThreshold float64       `validate:"gte=0,lte=1"`
	FailureProbabilityEstimateTimeout     time.Duration `validate:"gte=0"`
}

// QueueQuarantinerConfig controls how scheduling from misbehaving queues is rate-limited.
// See internal/scheduler/quarantine/queue_quarantiner.go for details.
type QueueQuarantinerConfig struct {
	QuarantineFactorMultiplier        float64       `validate:"gte=0,lte=1"`
	FailureProbabilityEstimateTimeout time.Duration `validate:"gte=0"`
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
