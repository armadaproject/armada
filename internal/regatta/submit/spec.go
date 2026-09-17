// Package submit implements regatta's own minimal job-submission runner. It deliberately does
// not reuse pkg/client/domain.LoadTestSpecification or client.ArmadaLoadTester.RunSubmissionTest
// - those belong to armada-load-tester's ramped, delayed, multi-submission-round load-testing
// model. Regatta only needs to submit a batch of jobs from one or more templates and wait for
// them to reach a terminal state, optionally spreading submission out via ramp-up.
package submit

import (
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/regatta/config"
)

// Spec is regatta's resolved submission batch, built from a scenario file's Load section via
// FromLoadConfig.
type Spec struct {
	Queue     string
	JobSetId  string
	Namespace string

	Jobs []JobItem

	Mode   string
	RampUp *RampUpParams
}

// JobItem is one job-spec template and how many copies of it to submit.
type JobItem struct {
	Spec  *v1.PodSpec
	Count int
}

// RampUpParams is config.RampUpConfig's two durations, already parsed.
type RampUpParams struct {
	RampDuration time.Duration
	StepInterval time.Duration
}

// FromLoadConfig converts a scenario file's Load section (already resolved by config.Load) into
// a Spec ready to hand to Run.
func FromLoadConfig(l config.Load) *Spec {
	jobs := make([]JobItem, 0, len(l.Jobs))
	for _, ref := range l.Jobs {
		jobs = append(jobs, JobItem{Spec: ref.ResolvedSpec, Count: ref.Count})
	}

	spec := &Spec{
		Queue:     l.Queue,
		JobSetId:  l.JobSetId,
		Namespace: l.Namespace,
		Jobs:      jobs,
		Mode:      l.Mode,
	}
	if l.RampUp != nil {
		spec.RampUp = &RampUpParams{
			RampDuration: l.RampUp.RampDurationParsed,
			StepInterval: l.RampUp.StepIntervalParsed,
		}
	}
	return spec
}
