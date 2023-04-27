package constraints

import (
	"testing"

	"github.com/stretchr/testify/require"

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
			ok, unschedulableReason, err := tc.constraints.CheckGlobalConstraints(tc.sctx)
			require.NoError(t, err)
			require.Equal(t, tc.globalUnschedulableReason == "", ok)
			require.Equal(t, tc.globalUnschedulableReason, unschedulableReason)

			ok, unschedulableReason, err = tc.constraints.CheckPerQueueAndPriorityClassConstraints(tc.sctx, tc.queue, tc.priorityClassName)
			require.NoError(t, err)
			require.Equal(t, tc.perQueueAndPriorityClassUnschedulableReason == "", ok)
			require.Equal(t, tc.perQueueAndPriorityClassUnschedulableReason, unschedulableReason)
		})
	}
}
