package reports

import "github.com/armadaproject/armada/internal/scheduler/schedulerobjects"

type LeaderSchedulingReportClientProvider interface {
	GetCurrentLeaderClient() schedulerobjects.SchedulerReportingClient
}

type KubernetesLeaderSchedulingReportClientProvider struct {
}
