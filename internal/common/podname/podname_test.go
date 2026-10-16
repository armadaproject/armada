package podname

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/pointer"
)

func TestResolve(t *testing.T) {
	tests := map[string]struct {
		stored  *string
		pending bool
		started bool
		want    string
	}{
		"a stored name wins over the job-scoped name":           {stored: pointer.String("team-run-1"), pending: true, started: true, want: "team-run-1"},
		"a stored name needs no pending or start time":          {stored: pointer.String("team-run-1"), want: "team-run-1"},
		"a run with a pending time gets the job-scoped name":    {pending: true, want: "armada-job-1-0"},
		"a run first seen running gets the job-scoped name":     {started: true, want: "armada-job-1-0"},
		"a run without a pending or start time has no pod name": {want: ""},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, Resolve("job-1", tc.stored, tc.pending, tc.started))
		})
	}
}
