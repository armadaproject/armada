package configuration

import (
	"time"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/executor/configuration/podchecks"
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
	ImpersonateUsers          bool
	TrackedNodeLabels         []string
	AvoidNodeLabelsOnRetry    []string
	ToleratedTaints           []string
	MinimumPodAge             time.Duration
	StuckTerminatingPodExpiry time.Duration
	FailedPodExpiry           time.Duration
	MinimumJobSize            common.ComputeResources
	PodDefaults               *PodDefaults
	PendingPodChecks          *podchecks.Checks
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

type ClientConfiguration struct {
	MaxMessageSizeBytes int
}

type ExecutorConfiguration struct {
	Metric        MetricConfiguration
	Application   ApplicationConfiguration
	ApiConnection client.ApiConnectionDetails
	Client        ClientConfiguration

	Kubernetes KubernetesConfiguration
	Task       TaskConfiguration
}
