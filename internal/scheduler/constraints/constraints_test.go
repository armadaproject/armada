package constraints

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
)

func TestConstraints(t *testing.T) {
	tests := map[string]struct {
		constraints                                 SchedulingConstraints
		sctx                                        *schedulercontext.SchedulingContext
		globalUnschedulableReason                   string
		queue                                       string
		priorityClassName                           string
		perQueueAndPriorityClassUnschedulableReason string
	}{} // TODO: Add tests.
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ok, unschedulableReason, err := tc.constraints.CheckRoundConstraints(tc.sctx)
			require.NoError(t, err)
			require.Equal(t, tc.globalUnschedulableReason == "", ok)
			require.Equal(t, tc.globalUnschedulableReason, unschedulableReason)

			ok, unschedulableReason, err = tc.constraints.CheckConstraints(tc.sctx, nil)
			require.NoError(t, err)
			require.Equal(t, tc.perQueueAndPriorityClassUnschedulableReason == "", ok)
			require.Equal(t, tc.perQueueAndPriorityClassUnschedulableReason, unschedulableReason)
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
