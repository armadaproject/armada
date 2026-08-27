package configuration

import (
	"time"

	"google.golang.org/grpc/keepalive"

	"github.com/armadaproject/armada/internal/common/observability"
	profilingconfig "github.com/armadaproject/armada/internal/common/profiling/configuration"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/executor/categorizer"
	"github.com/armadaproject/armada/internal/executor/configuration/podchecks"
	"github.com/armadaproject/armada/pkg/client"
)

type ApplicationConfiguration struct {
	// ClusterId is the unique identifier for the cluster that the executor is running on.
	// It is used to identify the cluster in the scheduler.
	ClusterId string
	Pool      string
	// The suffix to be added to the pool if the node is reserved
	// If set to empty string, no suffix will be added
	ReservedNodePoolSuffix string
	SubmitConcurrencyLimit int
	UpdateConcurrencyLimit int
	DeleteConcurrencyLimit int
	JobLeaseRequestTimeout time.Duration
	// MaxLeasedJobs is the maximum jobs the executor should have in Leased state ay any one time (i.e jobs not submitted to kubernetes)
	// It is largely used to calculate how many new jobs to request from the scheduler
	MaxLeasedJobs int
	// ErrorCategories defines category rules for classifying pod failures.
	// Set ErrorCategories.Enabled to true to turn classification on.
	ErrorCategories categorizer.ErrorCategoriesConfig `yaml:"errorCategories"`
	DebugEvents     DebugEventsConfig                 `yaml:"debugEvents"`
}

// DebugEventsConfig controls capture of pod diagnostics. Experimental - shape subject to change.
type DebugEventsConfig struct {
	// Enabled gates all capture. When false the debug field is left empty, as it was before pod
	// diagnostics existed. Every event is retained in the event ingester's Redis regardless of type,
	// so a multi-KiB payload attached per pod is the volume risk that warrants a kill switch.
	Enabled bool `yaml:"enabled"`
	// MinAppContainerRuntimeForFailureDebug is the runtime above which a failed pod's debug data is
	// captured, since a pod that fails after running this long has a failure its exit code alone
	// does not explain. Below it the failure is most likely the submitted code's own - a bad argument
	// or an immediate crash - which the exit code and termination message already describe, and which
	// can arrive a million at a time. This does not gate the capture for a pod whose app container
	// never started, which always carries debug data.
	MinAppContainerRuntimeForFailureDebug time.Duration `yaml:"minAppContainerRuntimeForFailureDebug"`

	Pod  PodDebugConfig  `yaml:"pod"`
	Node NodeDebugConfig `yaml:"node"`
}

type PodDebugConfig struct {
	// MaxEvents is the most recent Kubernetes events to capture. Events are the only unbounded part
	// of the payload, so they are what its size is tuned by: measured against a production node, 10
	// pod and 10 node events render to ~10 KiB of JSON that compresses to ~1.8 KiB for storage.
	// Roughly 300 B of raw JSON per event.
	MaxEvents int `yaml:"maxEvents"`
}

type NodeDebugConfig struct {
	MaxEvents int `yaml:"maxEvents"`
	// Labels and Annotations to capture, by exact name. Allowlisted rather than captured wholesale
	// because nodes carry a lot of both, most of it irrelevant to why a pod would not terminate.
	Labels      []string `yaml:"labels"`
	Annotations []string `yaml:"annotations"`
}

type PodDefaults struct {
	SchedulerName string
	Ingress       *IngressConfiguration
}

type StateChecksConfiguration struct {
	// Once a pod is submitted to kubernetes, this is how long we'll wait for it to appear in the kubernetes informer state
	// If the pod hasn't appeared after this duration, it is considered missing
	DeadlineForSubmittedPodConsideredMissing time.Duration
	// Once the executor has seen a pod appear on the cluster, it considers that run Active
	// If we get into a state where there is no longer a pod backing that Active run, this is how long we'll wait before we consider the pod missing
	// The most likely cause of this is actually a bug in the executors processing of the kubernetes state
	// However without it - we can have runs get indefinitely stuck as Active with no backing pod
	DeadlineForActivePodConsideredMissing time.Duration
}

type IngressConfiguration struct {
	HostnameSuffix string
	CertNameSuffix string
	Annotations    map[string]string
}

type ClientConfiguration struct {
	MaxMessageSizeBytes int
}

type KubernetesConfiguration struct {
	// Whether to impersonate users when creating Kubernetes objects.
	ImpersonateUsers bool
	// Max number of Kubernetes API queries per second
	// and max number of concurrent Kubernetes API queries.
	QPS           float32
	Burst         int
	Etcd          EtcdConfiguration
	NodePoolLabel string
	NodeTypeLabel string
	NodeIdLabel   string
	// TrackedNodeLabels is a list of node labels that the executor should index and track.
	// As nodes can have many labels, taking all of them into consideration can be slow down scheduling.
	// Only node labels defined in this list can be referenced in Armada job nodeSelector field.
	TrackedNodeLabels      []string
	AvoidNodeLabelsOnRetry []string
	// ToleratedTaints specifies taints which are tolerated by the executor.
	// If a node has a taint that is not in this list, the executor will consider it for scheduling Armada jobs.
	ToleratedTaints           []string
	MinimumPodAge             time.Duration
	StuckTerminatingPodExpiry time.Duration
	FailedPodExpiry           time.Duration
	MaxTerminatedPods         int
	PodDefaults               *PodDefaults
	StateChecks               StateChecksConfiguration
	FailedPodChecks           podchecks.FailedChecks
	PendingPodChecks          *podchecks.Checks
	FatalPodSubmissionErrors  []string
	// Minimum amount of resources marked as allocated to non-Armada pods on each node.
	// I.e., if the total resources allocated to non-Armada pods on some node drops below this value,
	// the executor adds a fictional allocation to make up the difference, such that the total is at least this.
	// Hence, specifying can ensure that, e.g., if a deamonset pod restarts, those resources are not considered for scheduling.
	MinimumResourcesMarkedAllocatedToNonArmadaPodsPerNode armadaresource.ComputeResources
	// When adding a fictional allocation to ensure resources allocated to non-Armada pods is at least
	// MinimumResourcesMarkedAllocatedToNonArmadaPodsPerNode, those resources are marked allocated at this priority.
	MinimumResourcesMarkedAllocatedToNonArmadaPodsPerNodePriority int32

	PodKillTimeout time.Duration
}

type EtcdConfiguration struct {
	// Etcd health monitoring configuration.
	// If provided, the executor monitors etcd health and stops requesting jobs while any etcd cluster is unhealthy.
	EtcdClustersHealthMonitoring []EtcdClusterHealthMonitoringConfiguration
}

// EtcdClusterHealthMonitoringConfiguration
// contains settings associated with monitoring the health of an etcd cluster.
type EtcdClusterHealthMonitoringConfiguration struct {
	// Etcd cluster name. Used in metrics exported by Armada.
	Name string `validate:"gt=0"`
	// Metric URLs of the etcd replicas making up this cluster.
	MetricUrls []string `validate:"gt=0"`
	// The cluster is considered unhealthy when for any replica in the cluster:
	// etcd_mvcc_db_total_size_in_use_in_bytes / etcd_server_quota_backend_bytes
	// > FractionOfStorageInUseLimit.
	FractionOfStorageInUseLimit float64 `validate:"gt=0,lte=1"`
	// The cluster is considered unhealthy when for any replica in the cluster:
	// etcd_mvcc_db_total_size_in_bytes / etcd_server_quota_backend_bytes
	// > FractionOfStorageLimit.
	FractionOfStorageLimit float64 `validate:"gt=0,lte=1"`
	// A replica is considered unavailable if the executor has failed to collect metrics from it for this amount of time.
	// The cluster is considered unhealthy if there are less than MinimumReplicasAvailable replicas available.
	ReplicaTimeout           time.Duration `validate:"gt=0"`
	MinimumReplicasAvailable int           `validate:"gt=0"`
	// Interval with which to scrape metrics from each etcd replica.
	ScrapeInterval time.Duration `validate:"gt=0"`
	// The time it takes to scrape metrics is exported as a prometheus histogram with exponential buckets.
	// These settings control the size and number of such buckets.
	ScrapeDelayBucketsStart  float64 `validate:"gt=0"`
	ScrapeDelayBucketsFactor float64 `validate:"gt=1"`
	ScrapeDelayBucketsCount  int     `validate:"gt=0"`
}

type TaskConfiguration struct {
	UtilisationReportingInterval          time.Duration
	MissingJobEventReconciliationInterval time.Duration
	JobLeaseRenewalInterval               time.Duration
	AllocateSpareClusterCapacityInterval  time.Duration
	PodIssueHandlingInterval              time.Duration
	PodDeletionInterval                   time.Duration
	QueueUsageDataRefreshInterval         time.Duration
	UtilisationEventProcessingInterval    time.Duration
	UtilisationEventReportingInterval     time.Duration
	ResourceCleanupInterval               time.Duration
	StateProcessorInterval                time.Duration
}

type MetricConfiguration struct {
	Port                    uint16
	ExposeQueueUsageMetrics bool
	CustomUsageMetrics      []CustomUsageMetrics
}

type CustomUsageMetrics struct {
	Namespace                  string
	EndpointSelectorLabelName  string
	EndpointSelectorLabelValue string
	Metrics                    []CustomUsageMetric
}

type CustomUsageMetric struct {
	Name                   string
	PrometheusMetricName   string
	PrometheusPodNameLabel string
	AggregateType          AggregateType
	Multiplier             float64
}

type AggregateType string

const (
	Sum  AggregateType = "Sum"
	Mean               = "Mean"
)

type ExecutorConfiguration struct {
	HttpPort uint16
	// If non-nil, net/http/pprof endpoints are exposed on localhost on this port.
	Profiling             *profilingconfig.ProfilingConfig
	Observability         observability.ObservabilityConfig
	Metric                MetricConfiguration
	Application           ApplicationConfiguration
	ExecutorApiConnection client.ApiConnectionDetails
	Client                ClientConfiguration
	GRPC                  keepalive.ClientParameters

	Kubernetes KubernetesConfiguration
	Task       TaskConfiguration
}
