package podname

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/pointer"
)

const jobId = "01hcq4x7d2k9m3n5p8r0s1t2v4"

// defaultName is the pod name of the job with the default settings.
const defaultName = "armada-" + jobId + "-0"

func TestForRun(t *testing.T) {
	tests := map[string]struct {
		prefix    string
		runScoped bool
		want      string
	}{
		"a job-scoped name with another prefix":                  {prefix: "team", want: "team-" + jobId + "-0"},
		"a run-scoped name with another prefix":                  {prefix: "team", runScoped: true, want: "team-run-1"},
		"a job without a prefix gets the default prefix":         {want: defaultName},
		"a run-scoped job without a prefix gets the default one": {runScoped: true, want: "armada-run-1"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, ForRun(tc.prefix, tc.runScoped, jobId, "run-1"))
		})
	}
}

func TestDefault_IsTheNameOfAJobWithoutSettings(t *testing.T) {
	assert.Equal(t, ForRun("", false, jobId, "run-1"), Default(jobId))
}

func TestResolve(t *testing.T) {
	tests := map[string]struct {
		stored  *string
		pending bool
		started bool
		want    string
	}{
		"a stored name wins over the job-scoped name":           {stored: pointer.String("team-run-1"), pending: true, started: true, want: "team-run-1"},
		"a stored name needs no pending or start time":          {stored: pointer.String("team-run-1"), want: "team-run-1"},
		"a run with a pending time gets the job-scoped name":    {pending: true, want: defaultName},
		"a run first seen running gets the job-scoped name":     {started: true, want: defaultName},
		"a run without a pending or start time has no pod name": {want: ""},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, Resolve(jobId, tc.stored, tc.pending, tc.started))
		})
	}
}
