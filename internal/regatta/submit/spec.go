// Package submit implements regatta's own minimal job-submission runner. It deliberately does
// not reuse pkg/client/domain.LoadTestSpecification or client.ArmadaLoadTester.RunSubmissionTest
// - those belong to armada-load-tester's ramped, delayed, multi-submission-round load-testing
// model. Regatta only needs to submit a batch of jobs per queue from one or more templates, on the
// schedule each queue's arrival asks for.
package submit

import (
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/internal/regatta/load"
)

// Spec is regatta's resolved submission, built from a scenario file's Load section via
// FromLoadConfig: one QueueSpec per queue.
type Spec struct {
	Queues []QueueSpec
}

// QueueSpec is one queue's jobs and when to submit them.
type QueueSpec struct {
	Queue          string
	PriorityFactor float64
	JobSetId       string
	Namespace      string
	Jobs           []JobItem
	// Steps is the submission schedule: how many jobs to submit at which offset from the start of the run.
	// The counts sum to the queue's total jobs.
	Steps []load.Step
	// Continuous is set instead of Jobs and Steps when the queue submits at a fixed rate until the run is
	// stopped.
	Continuous *ContinuousSpec
}

// ContinuousSpec is a queue that submits a batch every Step, for as long as the run lasts.
type ContinuousSpec struct {
	Step  time.Duration
	Rates []JobRate
}

// JobRate is one job shape's rate: jobs per step, which may be fractional. The submitter carries the
// remainder from step to step, so the long-run rate is exact.
type JobRate struct {
	Spec    *v1.PodSpec
	PerStep float64
}

// JobItem is one job-spec template and how many copies of it to submit.
type JobItem struct {
	Spec  *v1.PodSpec
	Count int
}

// TotalJobs is the number of jobs the queue submits.
func (q QueueSpec) TotalJobs() int {
	total := 0
	for _, j := range q.Jobs {
		total += j.Count
	}
	return total
}

// Continuous reports whether any queue submits until the run is stopped.
func (s *Spec) Continuous() bool {
	for _, q := range s.Queues {
		if q.Continuous != nil {
			return true
		}
	}
	return false
}

// TotalJobs is the number of jobs across every bounded queue; continuous queues add nothing.
func (s *Spec) TotalJobs() int {
	total := 0
	for _, q := range s.Queues {
		total += q.TotalJobs()
	}
	return total
}

// FromLoadConfig converts a scenario file's Load section (already normalised by config.LoadScenario)
// into a Spec ready to hand to Run, planning each queue's submission steps.
func FromLoadConfig(l config.Load) (*Spec, error) {
	spec := &Spec{Queues: make([]QueueSpec, 0, len(l.Queues))}
	for _, q := range l.Queues {
		if q.Continuous != nil {
			rates := make([]JobRate, len(q.Continuous.Rates))
			for i, r := range q.Continuous.Rates {
				rates[i] = JobRate{Spec: r.ResolvedSpec, PerStep: r.PerStep}
			}
			spec.Queues = append(spec.Queues, QueueSpec{
				Queue:          q.Name,
				PriorityFactor: q.PriorityFactor,
				JobSetId:       q.JobSetId,
				Namespace:      q.Namespace,
				Continuous:     &ContinuousSpec{Step: q.Continuous.Step, Rates: rates},
			})
			continue
		}
		jobs := make([]JobItem, 0, len(q.Jobs))
		for _, ref := range q.Jobs {
			jobs = append(jobs, JobItem{Spec: ref.ResolvedSpec, Count: ref.Count})
		}
		qs := QueueSpec{
			Queue:          q.Name,
			PriorityFactor: q.PriorityFactor,
			JobSetId:       q.JobSetId,
			Namespace:      q.Namespace,
			Jobs:           jobs,
		}
		steps, err := q.Schedule.Plan(qs.TotalJobs())
		if err != nil {
			return nil, fmt.Errorf("queue %q: %w", q.Name, err)
		}
		qs.Steps = steps
		spec.Queues = append(spec.Queues, qs)
	}
	return spec, nil
}
