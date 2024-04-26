package constraints

import (
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"

	"golang.org/x/time/rate"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
)

type constraintTest struct {
	constraints                         SchedulingConstraints
	sctx                                *schedulercontext.SchedulingContext
	gctx                                *schedulercontext.GangSchedulingContext
	queue                               string
	priorityClassName                   string
	expectedCheckRoundConstraintsReason string
	expectedCheckConstraintsReason      string
}

func TestConstraints(t *testing.T) {
	tests := map[string]*constraintTest{
		"no-constraints": makeConstraintsTest(NewSchedulingConstraints(
			"pool-1",
			makeResourceList("1000", "1000Gi"),
			makeResourceList("0", "0"),
			makeSchedulingConfig(),
			[]queue.Queue{},
		)),
		"empty-queue-constraints": makeConstraintsTest(NewSchedulingConstraints(
			"pool-1",
			makeResourceList("1000", "1000Gi"),
			makeResourceList("0", "0"),
			makeSchedulingConfig(),
			[]queue.Queue{{Name: "queue-1", ResourceLimitsByPriorityClassName: map[string]api.PriorityClassResourceLimits{}}},
		)),
		"within-constraints": makeConstraintsTest(NewSchedulingConstraints(
			"pool-1",
			makeResourceList("1000", "1000Gi"),
			makeResourceList("0", "0"),
			configuration.SchedulingConfig{
				MaximumResourceFractionToSchedule: map[string]float64{"cpu": 0.1, "memory": 0.1},
				MaxQueueLookback:                  1000,
				PriorityClasses:                   map[string]types.PriorityClass{"priority-class-1": {MaximumResourceFractionPerQueueByPool: map[string]map[string]float64{"pool-1": {"cpu": 0.9, "memory": 0.9}}}},
			},
			[]queue.Queue{{Name: "queue-1", ResourceLimitsByPriorityClassName: map[string]api.PriorityClassResourceLimits{"priority-class-1": {MaximumResourceFraction: map[string]float64{"cpu": 0.9, "memory": 0.9}}}}},
		)),
		"exceeds-queue-priority-class-constraint": func() *constraintTest {
			t := makeConstraintsTest(NewSchedulingConstraints(
				"pool-1",
				makeResourceList("1000", "1000Gi"),
				makeResourceList("0", "0"),
				makeSchedulingConfig(),
				[]queue.Queue{
					{
						Name: "queue-1",
						ResourceLimitsByPriorityClassName: map[string]api.PriorityClassResourceLimits{
							"priority-class-1": {
								MaximumResourceFraction: map[string]float64{"cpu": 0.000001, "memory": 0.9},
							},
						},
					},
				},
			))
			t.expectedCheckConstraintsReason = "per-queue resource limit exceeded"
			return t
		}(),
		"exceeds-queue-priority-class-pool-constraint": func() *constraintTest {
			t := makeConstraintsTest(NewSchedulingConstraints(
				"pool-1",
				makeResourceList("1000", "1000Gi"),
				makeResourceList("0", "0"),
				makeSchedulingConfig(),
				[]queue.Queue{
					{
						Name: "queue-1",
						ResourceLimitsByPriorityClassName: map[string]api.PriorityClassResourceLimits{
							"priority-class-1": {
								MaximumResourceFractionByPool: map[string]api.PriorityClassPoolResourceLimits{
									"pool-1": {
										MaximumResourceFraction: map[string]float64{"cpu": 0.000001, "memory": 0.9},
									},
								},
							},
						},
					},
				},
			))
			t.expectedCheckConstraintsReason = "per-queue resource limit exceeded"
			return t
		}(),
		"exceeds-priority-class-constraint": func() *constraintTest {
			t := makeConstraintsTest(NewSchedulingConstraints(
				"pool-1",
				makeResourceList("1000", "1000Gi"),
				makeResourceList("0", "0"),
				configuration.SchedulingConfig{
					MaximumResourceFractionToSchedule: map[string]float64{"cpu": 0.1, "memory": 0.1},
					MaxQueueLookback:                  1000,
					PriorityClasses:                   map[string]types.PriorityClass{"priority-class-1": {MaximumResourceFractionPerQueueByPool: map[string]map[string]float64{"pool-1": {"cpu": 0.00000001, "memory": 0.9}}}},
				},
				[]queue.Queue{},
			))
			t.expectedCheckConstraintsReason = "resource limit exceeded"
			return t
		}(),
		"priority-class-constraint-ignored-if-there-is-a-queue-constraint": makeConstraintsTest(NewSchedulingConstraints(
			"pool-1",
			makeResourceList("1000", "1000Gi"),
			makeResourceList("0", "0"),
			configuration.SchedulingConfig{
				MaximumResourceFractionToSchedule: map[string]float64{"cpu": 0.1, "memory": 0.1},
				MaxQueueLookback:                  1000,
				PriorityClasses:                   map[string]types.PriorityClass{"priority-class-1": {MaximumResourceFractionPerQueueByPool: map[string]map[string]float64{"pool-1": {"cpu": 0.00000001, "memory": 0.9}}}},
			},
			[]queue.Queue{{Name: "queue-1", ResourceLimitsByPriorityClassName: map[string]api.PriorityClassResourceLimits{"priority-class-1": {MaximumResourceFraction: map[string]float64{"cpu": 0.9, "memory": 0.9}}}}},
		)),
		"below-minimum-job-size": func() *constraintTest {
			t := makeConstraintsTest(NewSchedulingConstraints(
				"pool-1",
				makeResourceList("1000", "1000Gi"),
				makeResourceList("5", "1Mi"),
				makeSchedulingConfig(),
				[]queue.Queue{},
			))
			t.expectedCheckConstraintsReason = "job requests 1 cpu, but the minimum is 5"
			return t
		}(),
		"above-maximum-resources-to-schedule": func() *constraintTest {
			t := makeConstraintsTest(NewSchedulingConstraints(
				"pool-1",
				makeResourceList("1000", "1000Gi"),
				makeResourceList("0", "0"),
				configuration.SchedulingConfig{
					MaximumResourceFractionToSchedule: map[string]float64{"cpu": 0.00001, "memory": 0.1},
					MaxQueueLookback:                  1000,
				},
				[]queue.Queue{},
			))
			t.expectedCheckRoundConstraintsReason = "maximum resources scheduled"
			return t
		}(),
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ok, unscheduledReason, err := tc.constraints.CheckRoundConstraints(tc.sctx, tc.queue)
			require.NoError(t, err)
			require.Equal(t, tc.expectedCheckRoundConstraintsReason == "", ok)
			require.Equal(t, tc.expectedCheckRoundConstraintsReason, unscheduledReason)

			ok, unscheduledReason, err = tc.constraints.CheckConstraints(tc.sctx, tc.gctx)
			require.NoError(t, err)
			require.Equal(t, tc.expectedCheckConstraintsReason == "", ok)
			require.Equal(t, tc.expectedCheckConstraintsReason, unscheduledReason)
		})
	}
}

func TestScaleQuantity(t *testing.T) {
	tests := map[string]struct {
		input    resource.Quantity
		f        float64
		expected resource.Quantity
	}{
		"one": {
			input:    resource.MustParse("1"),
			f:        1,
			expected: resource.MustParse("1"),
		},
		"zero": {
			input:    resource.MustParse("1"),
			f:        0,
			expected: resource.MustParse("0"),
		},
		"rounding": {
			input:    resource.MustParse("1"),
			f:        0.3006,
			expected: resource.MustParse("301m"),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.True(t, tc.expected.Equal(ScaleQuantity(tc.input, tc.f)), "expected %s, but got %s", tc.expected.String(), tc.input.String())
		})
	}
}

func makeConstraintsTest(constraints SchedulingConstraints) *constraintTest {
	return &constraintTest{
		constraints: constraints,
		sctx: &schedulercontext.SchedulingContext{
			Pool:               "pool-1",
			WeightSum:          100,
			ScheduledResources: makeResourceList("1", "1Gi"),
			Limiter:            rate.NewLimiter(1e9, 1e6),
			QueueSchedulingContexts: map[string]*schedulercontext.QueueSchedulingContext{
				"queue-1": {
					Queue:                    "queue-1",
					Weight:                   1,
					Limiter:                  rate.NewLimiter(1e9, 1e6),
					Allocated:                makeResourceList("30", "1Gi"),
					AllocatedByPriorityClass: schedulerobjects.QuantityByTAndResourceType[string]{"priority-class-1": makeResourceList("20", "1Gi")},
				},
			},
			Started: time.Now(),
		},
		gctx: &schedulercontext.GangSchedulingContext{
			GangInfo: schedulercontext.GangInfo{
				PriorityClassName: "priority-class-1",
			},
			Queue:                 "queue-1",
			TotalResourceRequests: makeResourceList("1", "1Gi"),
			JobSchedulingContexts: []*schedulercontext.JobSchedulingContext{{}},
		},
		queue:                               "queue-1",
		priorityClassName:                   "priority-class-1",
		expectedCheckConstraintsReason:      "",
		expectedCheckRoundConstraintsReason: "",
	}
}

func makeSchedulingConfig() configuration.SchedulingConfig {
	return configuration.SchedulingConfig{
		MaximumResourceFractionToSchedule: map[string]float64{"cpu": 0.1, "memory": 0.1},
		MaxQueueLookback:                  1000,
		PriorityClasses:                   map[string]types.PriorityClass{"priority-class-1": {}},
	}
}

func makeResourceList(cpu string, memory string) schedulerobjects.ResourceList {
	return schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse(cpu), "memory": resource.MustParse(memory)}}
}
