package configuration

import (
	"time"

	"google.golang.org/grpc/keepalive"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/executor/configuration/podchecks"
	"github.com/armadaproject/armada/pkg/client"
)

type ApplicationConfiguration struct {
	ClusterId              string
	Pool                   string
	SubmitConcurrencyLimit int
	UpdateConcurrencyLimit int
	DeleteConcurrencyLimit int
	UseExecutorApi         bool
	UseLegacyApi           bool
	JobLeaseRequestTimeout time.Duration
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
	// Wether to impersonate users when creating Kubernetes objects.
	ImpersonateUsers bool
	// Max number of Kubernetes API queries per second
	// and max number of concurrent Kubernetes API queries.
	QPS                       float32
	Burst                     int
	Etcd                      EtcdConfiguration
	NodeIdLabel               string
	TrackedNodeLabels         []string
	AvoidNodeLabelsOnRetry    []string
	ToleratedTaints           []string
	MinimumPodAge             time.Duration
	StuckTerminatingPodExpiry time.Duration
	FailedPodExpiry           time.Duration
	MaxTerminatedPods         int
	MinimumJobSize            armadaresource.ComputeResources
	PodDefaults               *PodDefaults
	StateChecks               StateChecksConfiguration
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
	PodKillTimeout                                                time.Duration
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
	HttpPort              uint16
	Metric                MetricConfiguration
	Application           ApplicationConfiguration
	ApiConnection         client.ApiConnectionDetails
	ExecutorApiConnection client.ApiConnectionDetails
	Client                ClientConfiguration
	GRPC                  keepalive.ClientParameters

	Kubernetes KubernetesConfiguration
	Task       TaskConfiguration
}
