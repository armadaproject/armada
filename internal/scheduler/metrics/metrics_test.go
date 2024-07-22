package metrics

import (
	"github.com/armadaproject/armada/internal/scheduler/schedulerresult"
	"testing"
)

func TestReportSchedulerResult(t *testing.T) {
	result := schedulerresult.SchedulerResult{
		PreemptedJobs:                nil,
		ScheduledJobs:                nil,
		NodeIdByJobId:                nil,
		SchedulingContexts:           nil,
		AdditionalAnnotationsByJobId: nil,
	}
}
