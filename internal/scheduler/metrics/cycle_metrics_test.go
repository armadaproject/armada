package metrics

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/fairness"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/schedulerresult"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

const epsilon = 1e-6

func TestReportStateTransitions(t *testing.T) {
	fairnessCostProvider, err := fairness.NewDominantResourceFairness(
		cpu(100),
		configuration.SchedulingConfig{DominantResourceFairnessResourcesToConsider: []string{"cpu"}})
	require.NoError(t, err)
	result := schedulerresult.SchedulerResult{
		SchedulingContexts: []*context.SchedulingContext{
			{
				Pool:                 "pool1",
				FairnessCostProvider: fairnessCostProvider,
				QueueSchedulingContexts: map[string]*context.QueueSchedulingContext{
					"queue1": {
						Allocated:         cpu(10),
						Demand:            cpu(20),
						CappedDemand:      cpu(15),
						AdjustedFairShare: 0.15,
						SuccessfulJobSchedulingContexts: map[string]*context.JobSchedulingContext{
							"job1": {
								Job: testfixtures.Test1Cpu4GiJob("queue1", testfixtures.PriorityClass0),
							},
							"job2": {
								Job: testfixtures.Test1Cpu4GiJob("queue1", testfixtures.PriorityClass0),
							},
						},
						UnsuccessfulJobSchedulingContexts: map[string]*context.JobSchedulingContext{
							"job2": {
								Job: testfixtures.Test1Cpu4GiJob("queue1", testfixtures.PriorityClass0),
							},
						},
					},
				},
			},
		},
	}

	m := newCycleMetrics()
	m.ReportSchedulerResult(result)

	poolQueue := []string{"pool1", "queue1"}

	consideredJobs := testutil.ToFloat64(m.consideredJobs.WithLabelValues(poolQueue...))
	assert.Equal(t, 3.0, consideredJobs, "consideredJobs")

	allocated := testutil.ToFloat64(m.actualShare.WithLabelValues(poolQueue...))
	assert.InDelta(t, 0.1, allocated, epsilon, "allocated")

	demand := testutil.ToFloat64(m.demand.WithLabelValues(poolQueue...))
	assert.InDelta(t, 0.2, demand, epsilon, "demand")

	cappedDemand := testutil.ToFloat64(m.cappedDemand.WithLabelValues(poolQueue...))
	assert.InDelta(t, 0.15, cappedDemand, epsilon, "cappedDemand")

	adjustedFairShare := testutil.ToFloat64(m.adjustedFairShare.WithLabelValues(poolQueue...))
	assert.InDelta(t, 0.15, adjustedFairShare, epsilon, "adjustedFairShare")

	fairnessError := testutil.ToFloat64(m.fairnessError.WithLabelValues("pool1"))
	assert.InDelta(t, 0.05, fairnessError, epsilon, "fairnessError")
}

func cpu(n int) schedulerobjects.ResourceList {
	return schedulerobjects.ResourceList{
		Resources: map[string]resource.Quantity{"cpu": resource.MustParse(fmt.Sprintf("%d", n))},
	}
}
