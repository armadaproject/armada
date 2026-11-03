// Package submit is regatta's job submitter: per queue, batches of jobs from templates, on the schedule the queue's
// arrival asks for.
package submit

import (
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/internal/regatta/load"
)

// Spec is the resolved submission, one QueueSpec per queue (see FromLoadConfig).
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
	// Steps is the submission schedule: how many jobs at which offset from the start. The counts sum to the total.
	Steps []load.Step
	// Continuous replaces Jobs and Steps when the queue submits at a fixed rate until stopped.
	Continuous *ContinuousSpec
}

// ContinuousSpec is a queue that submits a batch every Step until the run is stopped.
type ContinuousSpec struct {
	Step  time.Duration
	Rates []JobRate
}

// JobRate is one job shape's rate, in jobs per step. It may be fractional: the submitter carries the remainder.
type JobRate struct {
	Spec    *v1.PodSpec
	PerStep float64
}

// JobItem is a job-spec template and how many copies of it to submit.
type JobItem struct {
	Spec  *v1.PodSpec
	Count int
}

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

// TotalJobs counts the jobs of the bounded queues.
func (s *Spec) TotalJobs() int {
	total := 0
	for _, q := range s.Queues {
		total += q.TotalJobs()
	}
	return total
}

// FromLoadConfig converts the (normalised) Load into a Spec, planning each queue's steps.
func FromLoadConfig(l config.Load) (*Spec, error) {
	spec := &Spec{Queues: make([]QueueSpec, 0, len(l.Queues))}
	for _, q := range l.Queues {
		if q.Continuous != nil {
			rates := make([]JobRate, len(q.Continuous.Rates))
			for i, r := range q.Continuous.Rates {
				rates[i] = JobRate{Spec: restrictToTargets(r.ResolvedSpec, q.Targets), PerStep: r.PerStep}
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
			jobs = append(jobs, JobItem{Spec: restrictToTargets(ref.ResolvedSpec, q.Targets), Count: ref.Count})
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

// restrictToTargets returns a copy of spec limited to the fake nodes of the named targets, by a required node affinity
// on TargetLabel added to every node selector term the spec has (terms are alternatives), or as its only term. With no
// targets, spec itself comes back. The argument is never modified.
func restrictToTargets(spec *v1.PodSpec, targets []string) *v1.PodSpec {
	if len(targets) == 0 || spec == nil {
		return spec
	}
	restricted := spec.DeepCopy()
	requirement := v1.NodeSelectorRequirement{
		Key:      config.TargetLabel,
		Operator: v1.NodeSelectorOpIn,
		Values:   append([]string(nil), targets...),
	}
	if restricted.Affinity == nil {
		restricted.Affinity = &v1.Affinity{}
	}
	if restricted.Affinity.NodeAffinity == nil {
		restricted.Affinity.NodeAffinity = &v1.NodeAffinity{}
	}
	nodeAffinity := restricted.Affinity.NodeAffinity
	if nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &v1.NodeSelector{}
	}
	required := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if len(required.NodeSelectorTerms) == 0 {
		required.NodeSelectorTerms = []v1.NodeSelectorTerm{{MatchExpressions: []v1.NodeSelectorRequirement{requirement}}}
		return restricted
	}
	for i := range required.NodeSelectorTerms {
		required.NodeSelectorTerms[i].MatchExpressions = append(required.NodeSelectorTerms[i].MatchExpressions, requirement)
	}
	return restricted
}
