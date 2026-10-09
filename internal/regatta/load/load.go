// Package load turns a scenario's distributions into exact job counts: how many jobs each queue of an entry
// gets, and when a queue's jobs are submitted. It is pure (no I/O, no randomness), so results repeat and
// counts always sum to the requested total.
//
// Both uses share one idea, a cumulative curve F over a range: queue k of Q takes the share
// F(k/Q) - F((k-1)/Q) of a total, and step k of n submits round(N*F(k/n)) - round(N*F((k-1)/n)) jobs.
package load

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"
)

// Distribution types.
const (
	Uniform   = "uniform"
	Gaussian  = "gaussian"
	LogNormal = "lognormal"
)

// Dist is a distribution type plus parameters, in the units of the axis it is applied to (queue numbers
// over queues, seconds over time). A bare string in a scenario file ("uniform") is a Dist with that type.
//
//	uniform:   no parameters
//	gaussian:  Mean, StdDev (defaults: the middle of the range, a sixth of it)
//	lognormal: Mu, Sigma, the mean and standard deviation of ln(x); both required
type Dist struct {
	Type   string   `json:"type,omitempty"`
	Mean   *float64 `json:"mean,omitempty"`
	StdDev *float64 `json:"stddev,omitempty"`
	Mu     *float64 `json:"mu,omitempty"`
	Sigma  *float64 `json:"sigma,omitempty"`
}

func (d *Dist) UnmarshalJSON(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err == nil {
		*d = Dist{Type: name}
		return nil
	}
	type plain Dist
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	*d = Dist(p)
	return nil
}

func (d Dist) EffectiveType() string {
	if d.Type == "" {
		return Uniform
	}
	return d.Type
}

// Curve is a cumulative distribution over a fraction of the range: Curve(0) is 0, Curve(1) is 1, and it
// never decreases.
type Curve func(x float64) float64

// Curve builds the cumulative curve over [0, max]: the number of queues, or the duration in seconds.
func (d Dist) Curve(max float64) (Curve, error) {
	if max <= 0 {
		return nil, fmt.Errorf("range must be positive, got %g", max)
	}
	switch d.EffectiveType() {
	case Uniform:
		if d.Mean != nil || d.StdDev != nil || d.Mu != nil || d.Sigma != nil {
			return nil, fmt.Errorf("uniform takes no parameters")
		}
		return func(x float64) float64 { return clamp01(x) }, nil

	case Gaussian:
		if d.Mu != nil || d.Sigma != nil {
			return nil, fmt.Errorf("gaussian takes mean and stddev, not mu and sigma")
		}
		mean, stddev := max/2, max/6
		if d.Mean != nil {
			mean = *d.Mean
		}
		if d.StdDev != nil {
			stddev = *d.StdDev
		}
		if stddev <= 0 {
			return nil, fmt.Errorf("gaussian stddev must be positive, got %g", stddev)
		}
		if mean < 0 || mean > max {
			return nil, fmt.Errorf("gaussian mean %g is outside the range [0, %g]", mean, max)
		}
		return rescaled(func(x float64) float64 { return normalCDF((x - mean) / stddev) }, max, "gaussian")

	case LogNormal:
		if d.Mean != nil || d.StdDev != nil {
			return nil, fmt.Errorf("lognormal takes mu and sigma, not mean and stddev")
		}
		if d.Mu == nil || d.Sigma == nil {
			return nil, fmt.Errorf("lognormal needs both mu and sigma")
		}
		mu, sigma := *d.Mu, *d.Sigma
		if sigma <= 0 {
			return nil, fmt.Errorf("lognormal sigma must be positive, got %g", sigma)
		}
		return rescaled(func(x float64) float64 {
			if x <= 0 {
				return 0
			}
			return normalCDF((math.Log(x) - mu) / sigma)
		}, max, "lognormal")

	default:
		return nil, fmt.Errorf("unknown distribution %q (want %s, %s or %s)", d.Type, Uniform, Gaussian, LogNormal)
	}
}

// rescaled stretches a CDF so it runs from 0 to 1 over [0, max], discarding the mass outside. It fails
// when nearly all the mass is outside.
func rescaled(cdf func(float64) float64, max float64, name string) (Curve, error) {
	lo, hi := cdf(0), cdf(max)
	if hi-lo < 1e-9 {
		return nil, fmt.Errorf("%s has almost no mass inside the range [0, %g]; check its parameters", name, max)
	}
	return func(x float64) float64 {
		return clamp01((cdf(clamp01(x)*max) - lo) / (hi - lo))
	}, nil
}

func normalCDF(z float64) float64 { return 0.5 * (1 + math.Erf(z/math.Sqrt2)) }

func clamp01(x float64) float64 { return math.Max(0, math.Min(1, x)) }

// Apportion splits total into integers that sum to exactly total, in proportion to shares (largest
// remainder, ties to the lower index). Every entry first gets minimum. It fails if total cannot cover the
// minimums.
func Apportion(shares []float64, total, minimum int) ([]int, error) {
	n := len(shares)
	if n == 0 {
		return nil, nil
	}
	if minimum < 0 || total < 0 {
		return nil, fmt.Errorf("total and minimum must not be negative")
	}
	if total < n*minimum {
		return nil, fmt.Errorf("total %d cannot give each of %d entries at least %d", total, n, minimum)
	}
	sum := 0.0
	for _, s := range shares {
		if s < 0 || math.IsNaN(s) {
			return nil, fmt.Errorf("shares must not be negative")
		}
		sum += s
	}
	if sum <= 0 {
		return nil, fmt.Errorf("shares must not all be zero")
	}

	remaining := total - n*minimum
	out := make([]int, n)
	fractions := make([]float64, n)
	assigned := 0
	for i, s := range shares {
		exact := s / sum * float64(remaining)
		whole := int(math.Floor(exact))
		out[i] = minimum + whole
		fractions[i] = exact - float64(whole)
		assigned += whole
	}
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return fractions[order[a]] > fractions[order[b]] })
	for i := 0; i < remaining-assigned; i++ {
		out[order[i%n]]++
	}
	return out, nil
}

// QueueShares returns the fraction of an entry's load each queue takes under d. They sum to 1.
func QueueShares(d Dist, queues int) ([]float64, error) {
	if queues < 1 {
		return nil, fmt.Errorf("need at least one queue")
	}
	curve, err := d.Curve(float64(queues))
	if err != nil {
		return nil, err
	}
	shares := make([]float64, queues)
	for k := 1; k <= queues; k++ {
		shares[k-1] = curve(float64(k)/float64(queues)) - curve(float64(k-1)/float64(queues))
	}
	return shares, nil
}

// QueueCounts returns how many of total jobs each queue gets under d, after every queue has minPerQueue.
func QueueCounts(d Dist, queues, total, minPerQueue int) ([]int, error) {
	shares, err := QueueShares(d, queues)
	if err != nil {
		return nil, err
	}
	return Apportion(shares, total, minPerQueue)
}

// Carry turns fractional per-step rates into whole jobs: Next adds the rates to what was carried over, emits
// the whole part and keeps the rest, so a rate of 0.3 yields a job every third or fourth step and the
// long-run total is exact. Not safe for concurrent use.
type Carry struct {
	rates []float64
	carry []float64
}

func NewCarry(rates []float64) *Carry {
	return &Carry{rates: append([]float64(nil), rates...), carry: make([]float64, len(rates))}
}

func (c *Carry) Next() []int {
	out := make([]int, len(c.rates))
	for i, rate := range c.rates {
		c.carry[i] += rate
		// The small epsilon keeps 0.1 + 0.1 + ... from falling a hair under a whole number.
		whole := math.Floor(c.carry[i] + 1e-9)
		out[i] = int(whole)
		c.carry[i] -= whole
	}
	return out
}

type Step struct {
	Offset time.Duration
	Count  int
}

// Schedule spreads a queue's jobs over time. Dist is uniform or gaussian. Duration 0 submits everything at
// once; continuous submission (negative Duration) is not planned here.
type Schedule struct {
	Dist     Dist
	Duration time.Duration
	Step     time.Duration
}

// Plan returns the steps that submit total jobs, skipping empty ones. Step k covers ((k-1)/n, k/n] of the
// duration and is submitted at the start of it.
func (s Schedule) Plan(total int) ([]Step, error) {
	if total < 0 {
		return nil, fmt.Errorf("total must not be negative")
	}
	if total == 0 {
		return nil, nil
	}
	if s.Duration < 0 {
		return nil, fmt.Errorf("continuous submission (negative duration) is not supported yet")
	}
	if s.Duration == 0 {
		return []Step{{Offset: 0, Count: total}}, nil
	}
	if s.Dist.EffectiveType() == LogNormal {
		return nil, fmt.Errorf("lognormal is not available over time, only over queues")
	}
	if s.Step <= 0 {
		return nil, fmt.Errorf("step must be positive")
	}
	steps := int(math.Round(float64(s.Duration) / float64(s.Step)))
	if steps < 1 {
		steps = 1
	}
	curve, err := s.Dist.Curve(s.Duration.Seconds())
	if err != nil {
		return nil, err
	}
	width := s.Duration / time.Duration(steps)

	plan := make([]Step, 0, steps)
	prev := 0
	for k := 1; k <= steps; k++ {
		cumulative := int(math.Round(float64(total) * curve(float64(k)/float64(steps))))
		if k == steps {
			cumulative = total
		}
		if count := cumulative - prev; count > 0 {
			plan = append(plan, Step{Offset: time.Duration(k-1) * width, Count: count})
		}
		prev = cumulative
	}
	return plan, nil
}
