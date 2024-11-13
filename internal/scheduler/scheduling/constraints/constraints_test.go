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
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

type constraintTest struct {
	constraints                         SchedulingConstraints
	sctx                                *context.SchedulingContext
	gctx                                *context.GangSchedulingContext
	queue                               string
	priorityClassName                   string
	expectedCheckRoundConstraintsReason string
	expectedCheckConstraintsReason      string
}

func TestConstraints(t *testing.T) {
	rlFactory, err := internaltypes.NewResourceListFactory([]configuration.ResourceType{
		{Name: "cpu"},
		{Name: "memory"},
		{Name: "a"},
		{Name: "b"},
		{Name: "c"},
		{Name: "d"},
	}, nil)
	assert.Nil(t, err)

	tests := map[string]*constraintTest{
		"no-constraints": makeConstraintsTest(
			NewSchedulingConstraints("pool-1", makeResourceList(rlFactory, "1000", "1000Gi"),
				makeSchedulingConfig(),
				[]*api.Queue{{Name: "queue-1"}},
			), rlFactory),
		"empty-queue-constraints": makeConstraintsTest(
			NewSchedulingConstraints("pool-1", makeResourceList(rlFactory, "1000", "1000Gi"),
				makeSchedulingConfig(),
				[]*api.Queue{{Name: "queue-1", Cordoned: false, ResourceLimitsByPriorityClassName: map[string]*api.PriorityClassResourceLimits{}}}), rlFactory),
		"within-constraints": makeConstraintsTest(NewSchedulingConstraints("pool-1", makeResourceList(rlFactory, "1000", "1000Gi"), configuration.SchedulingConfig{
			MaximumResourceFractionToSchedule: map[string]float64{"cpu": 0.1, "memory": 0.1},
			PriorityClasses:                   map[string]types.PriorityClass{"priority-class-1": {MaximumResourceFractionPerQueueByPool: map[string]map[string]float64{"pool-1": {"cpu": 0.9, "memory": 0.9}}}},
		}, []*api.Queue{{Name: "queue-1", Cordoned: false, ResourceLimitsByPriorityClassName: map[string]*api.PriorityClassResourceLimits{"priority-class-1": {MaximumResourceFraction: map[string]float64{"cpu": 0.9, "memory": 0.9}}}}}), rlFactory),
		"exceeds-queue-priority-class-constraint": func() *constraintTest {
			t := makeConstraintsTest(NewSchedulingConstraints("pool-1", makeResourceList(rlFactory, "1000", "1000Gi"), makeSchedulingConfig(), []*api.Queue{
				{
					Name:     "queue-1",
					Cordoned: false,
					ResourceLimitsByPriorityClassName: map[string]*api.PriorityClassResourceLimits{
						"priority-class-1": {
							MaximumResourceFraction: map[string]float64{"cpu": 0.000001, "memory": 0.9},
						},
					},
				},
			}), rlFactory)
			t.expectedCheckConstraintsReason = "resource limit exceeded"
			return t
		}(),
		"exceeds-queue-priority-class-pool-constraint": func() *constraintTest {
			t := makeConstraintsTest(NewSchedulingConstraints("pool-1", makeResourceList(rlFactory, "1000", "1000Gi"), makeSchedulingConfig(), []*api.Queue{
				{
					Name:     "queue-1",
					Cordoned: false,
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
			}), rlFactory)
			t.expectedCheckConstraintsReason = "resource limit exceeded"
			return t
		}(),
		"exceeds-priority-class-constraint": func() *constraintTest {
			t := makeConstraintsTest(NewSchedulingConstraints("pool-1", makeResourceList(rlFactory, "1000", "1000Gi"), configuration.SchedulingConfig{
				MaximumResourceFractionToSchedule: map[string]float64{"cpu": 0.1, "memory": 0.1},
				PriorityClasses:                   map[string]types.PriorityClass{"priority-class-1": {MaximumResourceFractionPerQueueByPool: map[string]map[string]float64{"pool-1": {"cpu": 0.00000001, "memory": 0.9}}}},
			}, []*api.Queue{{Name: "queue-1"}}), rlFactory)
			t.expectedCheckConstraintsReason = "resource limit exceeded"
			return t
		}(),
		"priority-class-constraint-ignored-if-there-is-a-queue-constraint": makeConstraintsTest(NewSchedulingConstraints("pool-1", makeResourceList(rlFactory, "1000", "1000Gi"), configuration.SchedulingConfig{
			MaximumResourceFractionToSchedule: map[string]float64{"cpu": 0.1, "memory": 0.1},
			PriorityClasses:                   map[string]types.PriorityClass{"priority-class-1": {MaximumResourceFractionPerQueueByPool: map[string]map[string]float64{"pool-1": {"cpu": 0.00000001, "memory": 0.9}}}},
		}, []*api.Queue{{Name: "queue-1", ResourceLimitsByPriorityClassName: map[string]*api.PriorityClassResourceLimits{"priority-class-1": {MaximumResourceFraction: map[string]float64{"cpu": 0.9, "memory": 0.9}}}}}), rlFactory),
		"one-constraint-per-level-falls-back-as-expected--within-limits": makeMultiLevelConstraintsTest(
			map[string]resource.Quantity{"a": resource.MustParse("99"), "b": resource.MustParse("19"), "c": resource.MustParse("2.9"), "d": resource.MustParse("0.39")},
			"",
			"",
			rlFactory,
		),
		"one-constraint-per-level-falls-back-as-expected--a-exceeds-limits": makeMultiLevelConstraintsTest(
			map[string]resource.Quantity{"a": resource.MustParse("101"), "b": resource.MustParse("19"), "c": resource.MustParse("2.9"), "d": resource.MustParse("0.39")},
			UnschedulableReasonMaximumResourcesExceeded,
			"",
			rlFactory,
		),
		"one-constraint-per-level-falls-back-as-expected--b-exceeds-limits": makeMultiLevelConstraintsTest(
			map[string]resource.Quantity{"a": resource.MustParse("99"), "b": resource.MustParse("21"), "c": resource.MustParse("2.9"), "d": resource.MustParse("0.39")},
			UnschedulableReasonMaximumResourcesExceeded,
			"",
			rlFactory,
		),
		"one-constraint-per-level-falls-back-as-expected--c-exceeds-limits": makeMultiLevelConstraintsTest(
			map[string]resource.Quantity{"a": resource.MustParse("99"), "b": resource.MustParse("19"), "c": resource.MustParse("3.1"), "d": resource.MustParse("0.39")},
			UnschedulableReasonMaximumResourcesExceeded,
			"",
			rlFactory,
		),
		"one-constraint-per-level-falls-back-as-expected--d-exceeds-limits": makeMultiLevelConstraintsTest(
			map[string]resource.Quantity{"a": resource.MustParse("99"), "b": resource.MustParse("19"), "c": resource.MustParse("2.9"), "d": resource.MustParse("0.41")},
			UnschedulableReasonMaximumResourcesExceeded,
			"",
			rlFactory,
		),
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ok, unscheduledReason, err := tc.constraints.CheckRoundConstraints(tc.sctx)
			require.NoError(t, err)
			require.Equal(t, tc.expectedCheckRoundConstraintsReason == "", ok)
			require.Equal(t, tc.expectedCheckRoundConstraintsReason, unscheduledReason)

			ok, unscheduledReason, err = tc.constraints.CheckJobConstraints(tc.sctx, tc.gctx)
			require.NoError(t, err)
			require.Equal(t, tc.expectedCheckConstraintsReason == "", ok)
			require.Equal(t, tc.expectedCheckConstraintsReason, unscheduledReason)
		})
	}
}

func TestCapResources(t *testing.T) {
	rlFactory := testfixtures.TestResourceListFactory
	tests := map[string]struct {
		constraints       SchedulingConstraints
		queue             string
		resources         map[string]internaltypes.ResourceList
		expectedResources map[string]internaltypes.ResourceList
	}{
		"no constraints": {
			constraints:       NewSchedulingConstraints("pool-1", makeResourceList(rlFactory, "1000", "1000Gi"), makeSchedulingConfig(), []*api.Queue{{Name: "queue-1"}}),
			queue:             "queue-1",
			resources:         map[string]internaltypes.ResourceList{"priority-class-1": makeResourceList(rlFactory, "1000", "1000Gi")},
			expectedResources: map[string]internaltypes.ResourceList{"priority-class-1": makeResourceList(rlFactory, "1000", "1000Gi")},
		},
		"unconstrained": {
			constraints: NewSchedulingConstraints("pool-1", makeResourceList(rlFactory, "1000", "1000Gi"), configuration.SchedulingConfig{
				PriorityClasses: map[string]types.PriorityClass{
					"priority-class-1": {
						MaximumResourceFractionPerQueueByPool: map[string]map[string]float64{
							"pool-1": {"cpu": 0.1, "memory": 0.9},
						},
					},
				},
			}, []*api.Queue{{Name: "queue-1"}}),
			queue:             "queue-1",
			resources:         map[string]internaltypes.ResourceList{"priority-class-1": makeResourceList(rlFactory, "1", "1Gi")},
			expectedResources: map[string]internaltypes.ResourceList{"priority-class-1": makeResourceList(rlFactory, "1", "1Gi")},
		},
		"per pool cap": {
			constraints: NewSchedulingConstraints("pool-1", makeResourceList(rlFactory, "1000", "1000Gi"), configuration.SchedulingConfig{
				PriorityClasses: map[string]types.PriorityClass{
					"priority-class-1": {
						MaximumResourceFractionPerQueueByPool: map[string]map[string]float64{
							"pool-1": {"cpu": 0.1, "memory": 0.9},
						},
					},
				},
			}, []*api.Queue{{Name: "queue-1"}}),
			queue:             "queue-1",
			resources:         map[string]internaltypes.ResourceList{"priority-class-1": makeResourceList(rlFactory, "1000", "1000Gi")},
			expectedResources: map[string]internaltypes.ResourceList{"priority-class-1": makeResourceList(rlFactory, "100", "100Gi")},
		},
		"per queue cap": {
			constraints: NewSchedulingConstraints("pool-1", makeResourceList(rlFactory, "1000", "1000Gi"), configuration.SchedulingConfig{
				PriorityClasses: map[string]types.PriorityClass{
					"priority-class-1": {
						MaximumResourceFractionPerQueueByPool: map[string]map[string]float64{
							"pool-1": {"cpu": 0.1, "memory": 0.9},
						},
					},
				},
			}, []*api.Queue{
				{
					Name: "queue-1",
					ResourceLimitsByPriorityClassName: map[string]*api.PriorityClassResourceLimits{
						"priority-class-1": {
							MaximumResourceFraction: map[string]float64{"cpu": 0.9, "memory": 0.9},
						},
					},
				},
			}),
			queue:             "queue-1",
			resources:         map[string]internaltypes.ResourceList{"priority-class-1": makeResourceList(rlFactory, "1000", "1000Gi")},
			expectedResources: map[string]internaltypes.ResourceList{"priority-class-1": makeResourceList(rlFactory, "900", "900Gi")},
		},
		"per queue cap with multi pc": {
			constraints: NewSchedulingConstraints("pool-1", makeResourceList(rlFactory, "1000", "1000Gi"), configuration.SchedulingConfig{
				PriorityClasses: map[string]types.PriorityClass{
					"priority-class-1": {
						MaximumResourceFractionPerQueueByPool: map[string]map[string]float64{
							"pool-1": {"cpu": 0.1, "memory": 0.9},
						},
					},
					"priority-class-2": {},
				},
			}, []*api.Queue{
				{
					Name: "queue-1",
					ResourceLimitsByPriorityClassName: map[string]*api.PriorityClassResourceLimits{
						"priority-class-1": {
							MaximumResourceFraction: map[string]float64{"cpu": 0.1, "memory": 0.1},
						},
						"priority-class-2": {
							MaximumResourceFraction: map[string]float64{"cpu": 0.9, "memory": 0.9},
						},
					},
				},
			}),
			queue: "queue-1",
			resources: map[string]internaltypes.ResourceList{
				"priority-class-1": makeResourceList(rlFactory, "1000", "1000Gi"),
				"priority-class-2": makeResourceList(rlFactory, "2000", "2000Gi"),
			},
			expectedResources: map[string]internaltypes.ResourceList{
				"priority-class-1": makeResourceList(rlFactory, "100", "100Gi"),
				"priority-class-2": makeResourceList(rlFactory, "900", "900Gi"),
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			capped := tc.constraints.CapResources(tc.queue, tc.resources)

			// Compare resources for equality. Note that we can't just do assert.Equal(tc.expectedResources, capped)
			// because the scale may have changed
			require.Equal(t, len(tc.expectedResources), len(capped), "number of priority classes differs")
			for pc, expectedCappedRl := range tc.expectedResources {
				cappedRl, ok := capped[pc]
				require.True(t, ok, "no resource list found for priority class %s", pc)
				require.True(t, expectedCappedRl.Equal(cappedRl), "capped resources (%s) not as expected (%s)", cappedRl.String(), expectedCappedRl.String())
			}
		})
	}
}

func makeMultiLevelConstraintsTest(requirements map[string]resource.Quantity, expectedCheckConstraintsReason string, expectedCheckRoundConstraintsReason string, rlFactory *internaltypes.ResourceListFactory) *constraintTest {
	rr := rlFactory.FromJobResourceListIgnoreUnknown(requirements)
	return &constraintTest{
		constraints: makeMultiLevelConstraints(rlFactory),
		sctx: &context.SchedulingContext{
			Pool:               "pool-1",
			WeightSum:          100,
			ScheduledResources: internaltypes.ResourceList{},
			Limiter:            rate.NewLimiter(1e9, 1e6),
			QueueSchedulingContexts: map[string]*context.QueueSchedulingContext{
				"queue-1": {
					Queue:     "queue-1",
					Weight:    1,
					Limiter:   rate.NewLimiter(1e9, 1e6),
					Allocated: internaltypes.ResourceList{},
					AllocatedByPriorityClass: map[string]internaltypes.ResourceList{
						"priority-class-1": rr,
					},
				},
			},
			Started: time.Now(),
		},
		gctx: &context.GangSchedulingContext{
			GangInfo: context.GangInfo{
				PriorityClassName: "priority-class-1",
			},
			Queue:                 "queue-1",
			TotalResourceRequests: rr,
			JobSchedulingContexts: []*context.JobSchedulingContext{{}},
		},
		queue:                               "queue-1",
		priorityClassName:                   "priority-class-1",
		expectedCheckConstraintsReason:      expectedCheckConstraintsReason,
		expectedCheckRoundConstraintsReason: expectedCheckRoundConstraintsReason,
	}
}

func makeMultiLevelConstraints(rlFactory *internaltypes.ResourceListFactory) SchedulingConstraints {
	return NewSchedulingConstraints("pool-1",
		rlFactory.FromNodeProto(
			map[string]resource.Quantity{
				"a": resource.MustParse("1000"),
				"b": resource.MustParse("1000"),
				"c": resource.MustParse("1000"),
				"d": resource.MustParse("1000"),
			},
		),
		configuration.SchedulingConfig{
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
		}, []*api.Queue{
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

func makeConstraintsTest(constraints SchedulingConstraints, rlFactory *internaltypes.ResourceListFactory) *constraintTest {
	return &constraintTest{
		constraints: constraints,
		sctx: &context.SchedulingContext{
			Pool:               "pool-1",
			WeightSum:          100,
			ScheduledResources: makeResourceList(rlFactory, "1", "1Gi"),
			Limiter:            rate.NewLimiter(1e9, 1e6),
			QueueSchedulingContexts: map[string]*context.QueueSchedulingContext{
				"queue-1": {
					Queue:     "queue-1",
					Weight:    1,
					Limiter:   rate.NewLimiter(1e9, 1e6),
					Allocated: makeResourceList(rlFactory, "30", "1Gi"),
					AllocatedByPriorityClass: map[string]internaltypes.ResourceList{
						"priority-class-1": makeResourceList(rlFactory, "20", "1Gi"),
					},
				},
			},
			Started: time.Now(),
		},
		gctx: &context.GangSchedulingContext{
			GangInfo: context.GangInfo{
				PriorityClassName: "priority-class-1",
			},
			Queue:                 "queue-1",
			TotalResourceRequests: makeResourceList(rlFactory, "1", "1Gi"),
			JobSchedulingContexts: []*context.JobSchedulingContext{{}},
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
		PriorityClasses:                   map[string]types.PriorityClass{"priority-class-1": {}},
	}
}

func makeResourceList(rlFactory *internaltypes.ResourceListFactory, cpu string, memory string) internaltypes.ResourceList {
	return rlFactory.FromNodeProto(
		map[string]resource.Quantity{
			"cpu":    resource.MustParse(cpu),
			"memory": resource.MustParse(memory),
		},
	)
}
