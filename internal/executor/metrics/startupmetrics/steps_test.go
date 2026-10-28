package startupmetrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestDeriveSteps(t *testing.T) {
	tests := map[string]struct {
		info     *armadaevents.PodStartUpInfo
		expected []measuredStep
	}{
		"nil info": {
			info: nil,
		},
		"both built-in steps": {
			info: fullyMeasuredInfo(),
			expected: []measuredStep{
				{name: schedulingStep, duration: 2 * time.Second},
				{name: sandboxCreationStep, duration: 7 * time.Second},
			},
		},
		"scheduled at not determined leaves neither step measurable": {
			info: &armadaevents.PodStartUpInfo{
				CreatedAt:      baselineWithOffset(0),
				SandboxReadyAt: baselineWithOffset(9),
			},
		},
		"sandbox ready at not determined, as on Kubernetes below 1.33": {
			info: &armadaevents.PodStartUpInfo{
				CreatedAt:   baselineWithOffset(0),
				ScheduledAt: baselineWithOffset(2),
			},
			expected: []measuredStep{{name: schedulingStep, duration: 2 * time.Second}},
		},
		"instants out of order are not reported as a step": {
			info: &armadaevents.PodStartUpInfo{
				CreatedAt:      baselineWithOffset(5),
				ScheduledAt:    baselineWithOffset(2),
				SandboxReadyAt: baselineWithOffset(9),
			},
			expected: []measuredStep{{name: sandboxCreationStep, duration: 7 * time.Second}},
		},
		"a configured step is a step of its own": {
			info: &armadaevents.PodStartUpInfo{
				CreatedAt:   baselineWithOffset(0),
				ScheduledAt: baselineWithOffset(2),
				ExtraSteps: []*armadaevents.StartUpStep{
					{Name: "infiniband", StartedAt: baselineWithOffset(10), FinishedAt: baselineWithOffset(25)},
					{Name: "cni", StartedAt: baselineWithOffset(4), FinishedAt: baselineWithOffset(6)},
				},
			},
			expected: []measuredStep{
				{name: schedulingStep, duration: 2 * time.Second},
				{name: "infiniband", duration: 15 * time.Second},
				{name: "cni", duration: 2 * time.Second},
			},
		},
		"a step missing an endpoint is not measured": {
			info: &armadaevents.PodStartUpInfo{
				ExtraSteps: []*armadaevents.StartUpStep{
					{Name: "half-measured", StartedAt: baselineWithOffset(10)},
					nil,
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, deriveSteps(tc.info))
		})
	}
}
