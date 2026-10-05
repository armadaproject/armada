package config

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/regatta/load"
)

type Load struct {
	Entries []QueueEntry `json:"queues"`

	Queues []QueueLoad `json:"-"` // Entries expanded to one per queue, filled by normalize
}

type QueueEntry struct {
	Prefix          string    `json:"prefix"`
	Count           int       `json:"count,omitempty"`
	TotalJobs       int       `json:"totalJobs"`
	JobsPerStep     float64   `json:"jobsPerStep,omitempty"` // required for continuous submission
	MinJobsPerQueue *int      `json:"minJobsPerQueue,omitempty"`
	Distribution    load.Dist `json:"distribution,omitempty"`   // type of distribution
	PriorityFactor  float64   `json:"priorityFactor,omitempty"` // on queue create, pre-existing queues won't get updated
	Namespace       string    `json:"namespace,omitempty"`      // job namespace, "default" if empty
	Targets         []string  `json:"targets,omitempty"`
	Arrival         *Arrival  `json:"arrival,omitempty"`
	Jobs            []JobRef  `json:"jobs"`
}

// QueueLoad is one queue expanded from an entry: the entry's settings plus exact counts.
type QueueLoad struct {
	Name           string
	PriorityFactor float64
	JobSetId       string // regatta-<Name>
	Namespace      string
	Targets        []string
	Arrival        *Arrival
	Jobs           []JobRef

	Schedule   load.Schedule   // Arrival resolved to durations; unused when continuous
	Continuous *ContinuousLoad // set instead of Jobs and Schedule
	Entry      string          // prefix of the entry this queue came from
}

// ContinuousLoad is a queue that submits every Step until the run is stopped.
type ContinuousLoad struct {
	Step  time.Duration
	Rates []JobRate
}

// JobRate is one job shape's rate.
type JobRate struct {
	JobSpec      string
	PerStep      float64 // jobs per step, fractional; the submitter carries the remainder between steps
	ResolvedSpec *v1.PodSpec
}

// TotalJobs is the number of jobs the queue submits; 0 for a continuous queue.
func (q QueueLoad) TotalJobs() int {
	total := 0
	for _, j := range q.Jobs {
		total += j.Count
	}
	return total
}

type JobRef struct {
	JobSpec string  `json:"jobSpec"`
	Share   float64 `json:"share"`

	Count        int         `json:"-"` // jobs of this shape in the expanded queue
	ResolvedSpec *v1.PodSpec `json:"-"` // loaded from JobSpec
}

type Arrival struct {
	Shape    string `json:"shape,omitempty"`
	Duration string `json:"duration,omitempty"`
	Step     string `json:"step,omitempty"`
	Mean     string `json:"mean,omitempty"`
	StdDev   string `json:"stddev,omitempty"`
}

const (
	// continuousMarker is the totalJobs and arrival.duration value that selects continuous submission.
	continuousMarker = -1

	defaultStep            = 10 * time.Second
	defaultContinuousStep  = time.Minute
	defaultMinJobsPerQueue = 1
	shareTolerance         = 1e-6
)

var queueNameRe = regexp.MustCompile(`^[A-Za-z0-9]([-_.A-Za-z0-9]*[A-Za-z0-9])?$`)

func (l Load) Names() []string {
	names := make([]string, len(l.Queues))
	for i, q := range l.Queues {
		names[i] = q.Name
	}
	return names
}

// Continuous reports whether any queue submits until stopped.
func (l Load) Continuous() bool {
	for _, q := range l.Queues {
		if q.Continuous != nil {
			return true
		}
	}
	return false
}

// TotalJobs is the number of jobs across the bounded queues.
func (l Load) TotalJobs() int {
	total := 0
	for _, q := range l.Queues {
		total += q.TotalJobs()
	}
	return total
}

// QueuesForTarget returns the queues that may land on the target; a queue with no targets matches all.
func (l Load) QueuesForTarget(target string) []QueueLoad {
	var out []QueueLoad
	for _, q := range l.Queues {
		if len(q.Targets) == 0 || containsString(q.Targets, target) {
			out = append(out, q)
		}
	}
	return out
}

func containsString(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

// normalize loads job specs, expands the entries into queues and validates them. dir is the scenario
// file's directory and targets the names of its execution targets.
func (l *Load) normalize(dir string, targets map[string]bool) error {
	if len(l.Entries) == 0 {
		return fmt.Errorf("load.queues needs at least one entry")
	}

	l.Queues = nil
	for i := range l.Entries {
		generated, err := l.Entries[i].expand(dir)
		if err != nil {
			return fmt.Errorf("load.queues[%d] (%q): %w", i, l.Entries[i].Prefix, err)
		}
		l.Queues = append(l.Queues, generated...)
	}

	seen := map[string]bool{}
	for i := range l.Queues {
		q := &l.Queues[i]
		if !queueNameRe.MatchString(q.Name) {
			return fmt.Errorf("queue %q: not a valid queue name (letters, digits, '-', '_' and '.', starting and ending with a letter or digit)", q.Name)
		}
		if seen[q.Name] {
			return fmt.Errorf("queue %q is declared more than once (every entry's queues share one namespace, so two prefixes can collide)", q.Name)
		}
		seen[q.Name] = true

		for _, t := range q.Targets {
			if !targets[t] {
				return fmt.Errorf("queue %q: targets names %q, which is not an execution target", q.Name, t)
			}
		}
	}
	return nil
}

// isContinuous reports whether totalJobs or arrival.duration is -1; expandContinuous requires both.
func (e *QueueEntry) isContinuous() bool {
	return e.TotalJobs == continuousMarker || (e.Arrival != nil && e.Arrival.Duration == strconv.Itoa(continuousMarker))
}

// expand turns the entry into queues with exact job counts (or rates, if continuous).
func (e *QueueEntry) expand(dir string) ([]QueueLoad, error) {
	if e.Prefix == "" {
		return nil, fmt.Errorf("prefix is required")
	}
	count := e.Count
	if count == 0 {
		count = 1
	}
	if count < 1 {
		return nil, fmt.Errorf("count must be at least 1")
	}
	if e.PriorityFactor != 0 && e.PriorityFactor < 1 {
		return nil, fmt.Errorf("priorityFactor must be at least 1 (Armada rejects lower values), got %g", e.PriorityFactor)
	}
	if count == 1 && e.Distribution != (load.Dist{}) {
		return nil, fmt.Errorf("distribution needs a count above 1; a single queue has nothing to split")
	}
	jobShares, err := e.loadJobShares(dir)
	if err != nil {
		return nil, err
	}
	priorityFactor := e.PriorityFactor
	if priorityFactor == 0 {
		priorityFactor = 1
	}
	width := len(strconv.Itoa(count))
	newQueue := func(k int) QueueLoad {
		name := fmt.Sprintf("%s%0*d", e.Prefix, width, k+1)
		return QueueLoad{
			Name:           name,
			PriorityFactor: priorityFactor,
			JobSetId:       "regatta-" + name,
			Namespace:      e.Namespace,
			Targets:        e.Targets,
			Arrival:        e.Arrival,
			Entry:          e.Prefix,
		}
	}

	if e.isContinuous() {
		return e.expandContinuous(count, jobShares, newQueue)
	}

	if e.JobsPerStep != 0 {
		return nil, fmt.Errorf("jobsPerStep only applies to continuous submission (totalJobs and arrival.duration both -1)")
	}
	if e.TotalJobs < 1 {
		return nil, fmt.Errorf("totalJobs must be at least 1 (or -1 for continuous submission)")
	}
	minPerQueue := defaultMinJobsPerQueue
	if e.MinJobsPerQueue != nil {
		minPerQueue = *e.MinJobsPerQueue
	}
	if minPerQueue < 0 {
		return nil, fmt.Errorf("minJobsPerQueue must not be negative")
	}
	schedule, err := e.Arrival.resolve()
	if err != nil {
		return nil, fmt.Errorf("arrival: %w", err)
	}
	counts, err := load.QueueCounts(e.Distribution, count, e.TotalJobs, minPerQueue)
	if err != nil {
		return nil, fmt.Errorf("distribution: %w", err)
	}

	queues := make([]QueueLoad, count)
	for k, total := range counts {
		mix, err := load.Apportion(jobShares, total, 0)
		if err != nil {
			return nil, err
		}
		jobs := make([]JobRef, 0, len(e.Jobs))
		for i, n := range mix {
			if n > 0 {
				jobs = append(jobs, JobRef{JobSpec: e.Jobs[i].JobSpec, Count: n, ResolvedSpec: e.Jobs[i].ResolvedSpec})
			}
		}
		queues[k] = newQueue(k)
		queues[k].Jobs = jobs
		queues[k].Schedule = schedule
	}
	return queues, nil
}

// expandContinuous gives each queue JobsPerStep times its queue share, times each job shape's share.
func (e *QueueEntry) expandContinuous(count int, jobShares []float64, newQueue func(k int) QueueLoad) ([]QueueLoad, error) {
	if e.TotalJobs != continuousMarker || e.Arrival == nil || e.Arrival.Duration != strconv.Itoa(continuousMarker) {
		return nil, fmt.Errorf("continuous submission needs both totalJobs and arrival.duration to be -1")
	}
	if e.JobsPerStep <= 0 {
		return nil, fmt.Errorf("continuous submission needs jobsPerStep greater than 0")
	}
	if e.MinJobsPerQueue != nil {
		return nil, fmt.Errorf("minJobsPerQueue does not apply to continuous submission (rates are fractional)")
	}
	step, err := e.Arrival.continuousStep()
	if err != nil {
		return nil, fmt.Errorf("arrival: %w", err)
	}
	queueShares, err := load.QueueShares(e.Distribution, count)
	if err != nil {
		return nil, fmt.Errorf("distribution: %w", err)
	}

	queues := make([]QueueLoad, count)
	for k, queueShare := range queueShares {
		rates := make([]JobRate, len(e.Jobs))
		for i, ref := range e.Jobs {
			rates[i] = JobRate{JobSpec: ref.JobSpec, PerStep: e.JobsPerStep * queueShare * jobShares[i], ResolvedSpec: ref.ResolvedSpec}
		}
		queues[k] = newQueue(k)
		queues[k].Continuous = &ContinuousLoad{Step: step, Rates: rates}
	}
	return queues, nil
}

func (e *QueueEntry) loadJobShares(dir string) ([]float64, error) {
	if len(e.Jobs) == 0 {
		return nil, fmt.Errorf("jobs must contain at least one entry")
	}
	if err := loadJobRefs(dir, e.Jobs); err != nil {
		return nil, err
	}
	shares := make([]float64, len(e.Jobs))
	sum := 0.0
	for i, ref := range e.Jobs {
		if ref.Share <= 0 {
			return nil, fmt.Errorf("jobs[%d] needs a share greater than 0", i)
		}
		shares[i] = ref.Share
		sum += ref.Share
	}
	if math.Abs(sum-1) > shareTolerance {
		return nil, fmt.Errorf("job shares must sum to 1, got %g", sum)
	}
	return shares, nil
}

func loadJobRefs(dir string, refs []JobRef) error {
	for i := range refs {
		refs[i].JobSpec = resolveRelative(dir, refs[i].JobSpec)
		spec, err := loadPodSpec(refs[i].JobSpec)
		if err != nil {
			return fmt.Errorf("jobs[%d]: %w", i, err)
		}
		refs[i].ResolvedSpec = spec
	}
	return nil
}

// continuousStep validates an arrival for continuous submission (uniform only) and returns its step.
func (a *Arrival) continuousStep() (time.Duration, error) {
	if a.Shape != "" && a.Shape != load.Uniform {
		return 0, fmt.Errorf("continuous submission is uniform only (a curve needs an end), got shape %q", a.Shape)
	}
	if a.Mean != "" || a.StdDev != "" {
		return 0, fmt.Errorf("mean and stddev apply only to shape gaussian")
	}
	if a.Step == "" {
		return defaultContinuousStep, nil
	}
	step, err := time.ParseDuration(a.Step)
	if err != nil {
		return 0, fmt.Errorf("step %q: %w", a.Step, err)
	}
	if step <= 0 {
		return 0, fmt.Errorf("step must be positive")
	}
	return step, nil
}

// resolve parses the arrival into a schedule. A nil arrival submits everything at once.
func (a *Arrival) resolve() (load.Schedule, error) {
	if a == nil {
		return load.Schedule{}, nil
	}
	var s load.Schedule

	switch a.Duration {
	case "", "0":
	case "-1":
		return s, fmt.Errorf("duration -1 (continuous submission) also needs totalJobs -1 and jobsPerStep")
	default:
		d, err := time.ParseDuration(a.Duration)
		if err != nil {
			return s, fmt.Errorf("duration %q: %w", a.Duration, err)
		}
		if d < 0 {
			return s, fmt.Errorf("duration must not be negative (use -1 with totalJobs -1 for continuous submission)")
		}
		s.Duration = d
	}

	s.Dist.Type = a.Shape
	switch s.Dist.EffectiveType() {
	case load.Uniform, load.Gaussian:
	case load.LogNormal:
		return s, fmt.Errorf("shape lognormal is only available over queues, not over time")
	default:
		return s, fmt.Errorf("unknown shape %q (want %s or %s)", a.Shape, load.Uniform, load.Gaussian)
	}
	if s.Duration == 0 {
		if a.Shape != "" && a.Shape != load.Uniform {
			return s, fmt.Errorf("shape %q needs a duration; it has nothing to spread over", a.Shape)
		}
		return s, nil
	}

	s.Step = defaultStep
	if a.Step != "" {
		step, err := time.ParseDuration(a.Step)
		if err != nil {
			return s, fmt.Errorf("step %q: %w", a.Step, err)
		}
		if step <= 0 {
			return s, fmt.Errorf("step must be positive")
		}
		s.Step = step
	}
	for _, p := range []struct {
		name  string
		value string
		dest  **float64
	}{{"mean", a.Mean, &s.Dist.Mean}, {"stddev", a.StdDev, &s.Dist.StdDev}} {
		if p.value == "" {
			continue
		}
		if s.Dist.EffectiveType() != load.Gaussian {
			return s, fmt.Errorf("%s only applies to shape gaussian", p.name)
		}
		d, err := time.ParseDuration(p.value)
		if err != nil {
			return s, fmt.Errorf("%s %q: %w", p.name, p.value, err)
		}
		seconds := d.Seconds()
		*p.dest = &seconds
	}
	// Build the curve once so bad parameters fail at load time, not mid-run.
	if _, err := s.Dist.Curve(s.Duration.Seconds()); err != nil {
		return s, err
	}
	return s, nil
}
