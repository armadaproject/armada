package configuration

import (
	"time"

	"google.golang.org/grpc/keepalive"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/executor/configuration/podchecks"
	"github.com/armadaproject/armada/pkg/client"
)

type ApplicationConfiguration struct {
	ClusterId              string
	Pool                   string
	SubmitConcurrencyLimit int
	UpdateConcurrencyLimit int
	DeleteConcurrencyLimit int
}

type PodDefaults struct {
	SchedulerName string
	Ingress       *IngressConfiguration
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
	TrackedNodeLabels         []string
	AvoidNodeLabelsOnRetry    []string
	ToleratedTaints           []string
	MinimumPodAge             time.Duration
	StuckTerminatingPodExpiry time.Duration
	FailedPodExpiry           time.Duration
	MaxTerminatedPods         int
	MinimumJobSize            common.ComputeResources
	PodDefaults               *PodDefaults
	PendingPodChecks          *podchecks.Checks
	FatalPodSubmissionErrors  []string
	// NodeReservedResources config is used to factor in reserved resources on each node
	// when validating can a job be scheduled on a node during job submit (i.e. factor in resources for daemonset pods)
	NodeReservedResources common.ComputeResources
	PodKillTimeout        time.Duration
}

type EtcdConfiguration struct {
	// URLs of the etcd instances storing the cluster state.
	// If provided, Armada monitors the health of etcd and
	// stops requesting jobs when etcd is EtcdFractionOfStorageInUseSoftLimit percent full and
	// stops pod creation when etcd is EtcdFractionOfStorageInUseHardLimit or more percent full.
	MetricUrls                      []string
	FractionOfStorageInUseSoftLimit float64
	FractionOfStorageInUseHardLimit float64
	// This is the number of etcd endpoints that have to be healthy for Armada to perform the health check
	// If less than MinimumAvailable are healthy, Armada will consider etcd unhealthy and stop submitting pods
	MinimumAvailable int
}

type TaskConfiguration struct {
	UtilisationReportingInterval          time.Duration
	MissingJobEventReconciliationInterval time.Duration
	JobLeaseRenewalInterval               time.Duration
	AllocateSpareClusterCapacityInterval  time.Duration
	PodDeletionInterval                   time.Duration
	QueueUsageDataRefreshInterval         time.Duration
	UtilisationEventProcessingInterval    time.Duration
	UtilisationEventReportingInterval     time.Duration
	ResourceCleanupInterval               time.Duration
}

type MetricConfiguration struct {
	Port                    uint16
	ExposeQueueUsageMetrics bool
}

type ExecutorConfiguration struct {
	HttpPort      uint16
	Metric        MetricConfiguration
	Application   ApplicationConfiguration
	ApiConnection client.ApiConnectionDetails
	Client        ClientConfiguration
	GRPC          keepalive.ClientParameters

	Kubernetes KubernetesConfiguration
	Task       TaskConfiguration
}
