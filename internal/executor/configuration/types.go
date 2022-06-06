package configuration

import (
	"time"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/executor/configuration/podchecks"
	"github.com/G-Research/armada/pkg/client"
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
	QPS   float32
	Burst int
	// URLs of the etcd instances storing the cluster state.
	// If provided, Armada monitors the health of etcd and
	// stops requesting jobs when etcd is EtcdFractionOfStorageInUseSoftLimit percent full and
	// stops pod creation when etcd is EtcdFractionOfStorageInUseHardLimit or more percent full.
	EtcdMetricUrls                      []string
	EtcdFractionOfStorageInUseSoftLimit float64
	EtcdFractionOfStorageInUseHardLimit float64
	TrackedNodeLabels                   []string
	AvoidNodeLabelsOnRetry              []string
	ToleratedTaints                     []string
	MinimumPodAge                       time.Duration
	StuckTerminatingPodExpiry           time.Duration
	FailedPodExpiry                     time.Duration
	MaxTerminatedPods                   int
	MinimumJobSize                      common.ComputeResources
	PodDefaults                         *PodDefaults
	PendingPodChecks                    *podchecks.Checks
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
	Metric        MetricConfiguration
	Application   ApplicationConfiguration
	ApiConnection client.ApiConnectionDetails
	Client        ClientConfiguration

	Kubernetes KubernetesConfiguration
	Task       TaskConfiguration
}
