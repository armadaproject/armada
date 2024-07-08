package constraints

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
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
			makeSchedulingConfig(),
			[]*api.Queue{},
		)),
		"empty-queue-constraints": makeConstraintsTest(NewSchedulingConstraints(
			"pool-1",
			makeResourceList("1000", "1000Gi"),
			makeSchedulingConfig(),
			[]*api.Queue{{Name: "queue-1", ResourceLimitsByPriorityClassName: map[string]*api.PriorityClassResourceLimits{}}},
		)),
		"within-constraints": makeConstraintsTest(NewSchedulingConstraints(
			"pool-1",
			makeResourceList("1000", "1000Gi"),
			configuration.SchedulingConfig{
				MaximumResourceFractionToSchedule: map[string]float64{"cpu": 0.1, "memory": 0.1},
				MaxQueueLookback:                  1000,
				PriorityClasses:                   map[string]types.PriorityClass{"priority-class-1": {MaximumResourceFractionPerQueueByPool: map[string]map[string]float64{"pool-1": {"cpu": 0.9, "memory": 0.9}}}},
			},
			[]*api.Queue{{Name: "queue-1", ResourceLimitsByPriorityClassName: map[string]*api.PriorityClassResourceLimits{"priority-class-1": {MaximumResourceFraction: map[string]float64{"cpu": 0.9, "memory": 0.9}}}}},
		)),
		"exceeds-queue-priority-class-constraint": func() *constraintTest {
			t := makeConstraintsTest(NewSchedulingConstraints(
				"pool-1",
				makeResourceList("1000", "1000Gi"),
				makeSchedulingConfig(),
				[]*api.Queue{
					{
						Name: "queue-1",
						ResourceLimitsByPriorityClassName: map[string]*api.PriorityClassResourceLimits{
							"priority-class-1": {
								MaximumResourceFraction: map[string]float64{"cpu": 0.000001, "memory": 0.9},
							},
						},
					},
				},
			))
			t.expectedCheckConstraintsReason = "resource limit exceeded"
			return t
		}(),
		"exceeds-queue-priority-class-pool-constraint": func() *constraintTest {
			t := makeConstraintsTest(NewSchedulingConstraints(
				"pool-1",
				makeResourceList("1000", "1000Gi"),
				makeSchedulingConfig(),
				[]*api.Queue{
					{
						Name: "queue-1",
						ResourceLimitsByPriorityClassName: map[string]*api.PriorityClassResourceLimits{
							"priority-class-1": {
								MaximumResourceFractionByPool: map[string]*api.PriorityClassPoolResourceLimits{
									"pool-1": {
										MaximumResourceFraction: map[string]float64{"cpu": 0.000001, "memory": 0.9},
									},
								},
							},
						},
					},
				},
			))
			t.expectedCheckConstraintsReason = "resource limit exceeded"
			return t
		}(),
		"exceeds-priority-class-constraint": func() *constraintTest {
			t := makeConstraintsTest(NewSchedulingConstraints(
				"pool-1",
				makeResourceList("1000", "1000Gi"),
				configuration.SchedulingConfig{
					MaximumResourceFractionToSchedule: map[string]float64{"cpu": 0.1, "memory": 0.1},
					MaxQueueLookback:                  1000,
					PriorityClasses:                   map[string]types.PriorityClass{"priority-class-1": {MaximumResourceFractionPerQueueByPool: map[string]map[string]float64{"pool-1": {"cpu": 0.00000001, "memory": 0.9}}}},
				},
				[]*api.Queue{},
			))
			t.expectedCheckConstraintsReason = "resource limit exceeded"
			return t
		}(),
		"priority-class-constraint-ignored-if-there-is-a-queue-constraint": makeConstraintsTest(NewSchedulingConstraints(
			"pool-1",
			makeResourceList("1000", "1000Gi"),
			configuration.SchedulingConfig{
				MaximumResourceFractionToSchedule: map[string]float64{"cpu": 0.1, "memory": 0.1},
				MaxQueueLookback:                  1000,
				PriorityClasses:                   map[string]types.PriorityClass{"priority-class-1": {MaximumResourceFractionPerQueueByPool: map[string]map[string]float64{"pool-1": {"cpu": 0.00000001, "memory": 0.9}}}},
			},
			[]*api.Queue{{Name: "queue-1", ResourceLimitsByPriorityClassName: map[string]*api.PriorityClassResourceLimits{"priority-class-1": {MaximumResourceFraction: map[string]float64{"cpu": 0.9, "memory": 0.9}}}}},
		)),
		"one-constraint-per-level-falls-back-as-expected--within-limits": makeMultiLevelConstraintsTest(
			map[string]resource.Quantity{"a": resource.MustParse("99"), "b": resource.MustParse("19"), "c": resource.MustParse("2.9"), "d": resource.MustParse("0.39")},
			"",
			"",
		),
		"one-constraint-per-level-falls-back-as-expected--a-exceeds-limits": makeMultiLevelConstraintsTest(
			map[string]resource.Quantity{"a": resource.MustParse("101"), "b": resource.MustParse("19"), "c": resource.MustParse("2.9"), "d": resource.MustParse("0.39")},
			UnschedulableReasonMaximumResourcesExceeded,
			"",
		),
		"one-constraint-per-level-falls-back-as-expected--b-exceeds-limits": makeMultiLevelConstraintsTest(
			map[string]resource.Quantity{"a": resource.MustParse("99"), "b": resource.MustParse("21"), "c": resource.MustParse("2.9"), "d": resource.MustParse("0.39")},
			UnschedulableReasonMaximumResourcesExceeded,
			"",
		),
		"one-constraint-per-level-falls-back-as-expected--c-exceeds-limits": makeMultiLevelConstraintsTest(
			map[string]resource.Quantity{"a": resource.MustParse("99"), "b": resource.MustParse("19"), "c": resource.MustParse("3.1"), "d": resource.MustParse("0.39")},
			UnschedulableReasonMaximumResourcesExceeded,
			"",
		),
		"one-constraint-per-level-falls-back-as-expected--d-exceeds-limits": makeMultiLevelConstraintsTest(
			map[string]resource.Quantity{"a": resource.MustParse("99"), "b": resource.MustParse("19"), "c": resource.MustParse("2.9"), "d": resource.MustParse("0.41")},
			UnschedulableReasonMaximumResourcesExceeded,
			"",
		),
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

func makeMultiLevelConstraintsTest(requirements map[string]resource.Quantity, expectedCheckConstraintsReason string, expectedCheckRoundConstraintsReason string) *constraintTest {
	zeroResources := schedulerobjects.ResourceList{
		Resources: map[string]resource.Quantity{"a": resource.MustParse("0"), "b": resource.MustParse("0"), "c": resource.MustParse("0"), "d": resource.MustParse("0")},
	}
	return &constraintTest{
		constraints: makeMultiLevelConstraints(),
		sctx: &schedulercontext.SchedulingContext{
			Pool:               "pool-1",
			WeightSum:          100,
			ScheduledResources: zeroResources.DeepCopy(),
			Limiter:            rate.NewLimiter(1e9, 1e6),
			QueueSchedulingContexts: map[string]*schedulercontext.QueueSchedulingContext{
				"queue-1": {
					Queue:     "queue-1",
					Weight:    1,
					Limiter:   rate.NewLimiter(1e9, 1e6),
					Allocated: zeroResources.DeepCopy(),
					AllocatedByPriorityClass: schedulerobjects.QuantityByTAndResourceType[string]{"priority-class-1": schedulerobjects.ResourceList{
						Resources: requirements,
					}},
				},
			},
			Started: time.Now(),
		},
		gctx: &schedulercontext.GangSchedulingContext{
			GangInfo: schedulercontext.GangInfo{
				PriorityClassName: "priority-class-1",
			},
			Queue:                 "queue-1",
			TotalResourceRequests: schedulerobjects.ResourceList{Resources: requirements},
			JobSchedulingContexts: []*schedulercontext.JobSchedulingContext{{}},
		},
		queue:                               "queue-1",
		priorityClassName:                   "priority-class-1",
		expectedCheckConstraintsReason:      expectedCheckConstraintsReason,
		expectedCheckRoundConstraintsReason: expectedCheckRoundConstraintsReason,
	}
}

func makeMultiLevelConstraints() SchedulingConstraints {
	return NewSchedulingConstraints(
		"pool-1",
		schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"a": resource.MustParse("1000"), "b": resource.MustParse("1000"), "c": resource.MustParse("1000"), "d": resource.MustParse("1000")}},
		configuration.SchedulingConfig{
			MaxQueueLookback: 1000,
			PriorityClasses: map[string]types.PriorityClass{
				"priority-class-1": {
					MaximumResourceFractionPerQueue: map[string]float64{
						"a": 0.0001, "b": 0.0002, "c": 0.0003, "d": 0.0004,
					},
					MaximumResourceFractionPerQueueByPool: map[string]map[string]float64{
						"pool-1": {
							"a": 0.001, "b": 0.002, "c": 0.003,
						},
					},
				},
			},
		},
		[]*api.Queue{
			{
				Name: "queue-1",
				ResourceLimitsByPriorityClassName: map[string]*api.PriorityClassResourceLimits{
					"priority-class-1": {
						MaximumResourceFraction: map[string]float64{"a": 0.01, "b": 0.02},
						MaximumResourceFractionByPool: map[string]*api.PriorityClassPoolResourceLimits{
							"pool-1": {
								MaximumResourceFraction: map[string]float64{"a": 0.1},
							},
						},
					},
				},
			},
		},
	)
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

func TestIsStrictlyLessOrEqual(t *testing.T) {
	tests := map[string]struct {
		a        map[string]resource.Quantity
		b        map[string]resource.Quantity
		expected bool
	}{
		"both empty": {
			a:        make(map[string]resource.Quantity),
			b:        make(map[string]resource.Quantity),
			expected: true,
		},
		"zero and missing is equal": {
			a: map[string]resource.Quantity{
				"foo": resource.MustParse("1"),
				"bar": resource.MustParse("0"),
			},
			b: map[string]resource.Quantity{
				"foo": resource.MustParse("1"),
			},
			expected: true,
		},
		"simple equal": {
			a: map[string]resource.Quantity{
				"cpu":    resource.MustParse("1"),
				"memory": resource.MustParse("2"),
				"foo":    resource.MustParse("3"),
			},
			b: map[string]resource.Quantity{
				"cpu":    resource.MustParse("1"),
				"memory": resource.MustParse("2"),
				"foo":    resource.MustParse("3"),
			},
			expected: true,
		},
		"simple true": {
			a: map[string]resource.Quantity{
				"foo": resource.MustParse("1"),
				"bar": resource.MustParse("2"),
			},
			b: map[string]resource.Quantity{
				"foo": resource.MustParse("1"),
				"bar": resource.MustParse("3"),
			},
			expected: true,
		},
		"simple false": {
			a: map[string]resource.Quantity{
				"foo": resource.MustParse("1"),
				"bar": resource.MustParse("3"),
			},
			b: map[string]resource.Quantity{
				"foo": resource.MustParse("1"),
				"bar": resource.MustParse("2"),
			},
			expected: false,
		},
		"present in a missing in b true": {
			a: map[string]resource.Quantity{
				"foo": resource.MustParse("1"),
				"bar": resource.MustParse("2"),
			},
			b: map[string]resource.Quantity{
				"foo": resource.MustParse("1"),
			},
			expected: true,
		},
		"missing in a present in b true": {
			a: map[string]resource.Quantity{
				"foo": resource.MustParse("1"),
			},
			b: map[string]resource.Quantity{
				"foo": resource.MustParse("1"),
				"bar": resource.MustParse("2"),
			},
			expected: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, isStrictlyLessOrEqual(tc.a, tc.b))
		})
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
