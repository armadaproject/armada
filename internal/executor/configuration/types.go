package configuration

import (
	"time"

	"github.com/G-Research/armada/pkg/client"
)

type ApplicationConfiguration struct {
	ClusterId string
}

type KubernetesConfiguration struct {
	ImpersonateUsers  bool
	TrackedNodeLabels []string
	MinimumPodAge     time.Duration
	FailedPodExpiry   time.Duration
}

type TaskConfiguration struct {
	UtilisationReportingInterval          time.Duration
	MissingJobEventReconciliationInterval time.Duration
	JobLeaseRenewalInterval               time.Duration
	AllocateSpareClusterCapacityInterval  time.Duration
	StuckPodScanInterval                  time.Duration
	PodDeletionInterval                   time.Duration
	PodMetricsInterval                    time.Duration
	QueueUsageDataRefreshInterval         time.Duration
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
