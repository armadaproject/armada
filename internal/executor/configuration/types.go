package configuration

import (
	"time"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/client"
)

type ApplicationConfiguration struct {
	ClusterId string
	Pool      string
}

type PodDefaults struct {
	SchedulerName string
	Ingress       *IngressConfiguration
}

type IngressConfiguration struct {
	HostnameSuffix string
	Annotations    map[string]string
}

type KubernetesConfiguration struct {
	ImpersonateUsers  bool
	TrackedNodeLabels []string
	ToleratedTaints   []string
	MinimumPodAge     time.Duration
	FailedPodExpiry   time.Duration
	StuckPodExpiry    time.Duration
	MinimumJobSize    common.ComputeResources
	PodDefaults       *PodDefaults
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
}

type MetricConfiguration struct {
	Port                    uint16
	ExposeQueueUsageMetrics bool
}

type ExecutorConfiguration struct {
	Metric        MetricConfiguration
	Application   ApplicationConfiguration
	ApiConnection client.ApiConnectionDetails

	Kubernetes KubernetesConfiguration
	Task       TaskConfiguration
}
