package load

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func f(v float64) *float64 { return &v }

func sum(xs []int) int {
	total := 0
	for _, x := range xs {
		total += x
	}
	return total
}

func TestDist_UnmarshalAcceptsAStringOrAnObject(t *testing.T) {
	var fromString Dist
	require.NoError(t, json.Unmarshal([]byte(`"gaussian"`), &fromString))
	require.Equal(t, Dist{Type: Gaussian}, fromString)

	var fromObject Dist
	require.NoError(t, json.Unmarshal([]byte(`{"type":"lognormal","mu":4.6,"sigma":1}`), &fromObject))
	require.Equal(t, LogNormal, fromObject.Type)
	require.Equal(t, 4.6, *fromObject.Mu)
	require.Equal(t, 1.0, *fromObject.Sigma)

	var empty Dist
	require.NoError(t, json.Unmarshal([]byte(`{}`), &empty))
	require.Equal(t, Uniform, empty.EffectiveType(), "no type means uniform")
}

func TestCurve_IsAValidCumulativeDistribution(t *testing.T) {
	for name, d := range map[string]Dist{
		"uniform":   {Type: Uniform},
		"gaussian":  {Type: Gaussian},
		"lognormal": {Type: LogNormal, Mu: f(math.Log(100)), Sigma: f(1)},
	} {
		t.Run(name, func(t *testing.T) {
			curve, err := d.Curve(1000)
			require.NoError(t, err)
			require.InDelta(t, 0, curve(0), 1e-9)
			require.InDelta(t, 1, curve(1), 1e-9)
			previous := 0.0
			for x := 0.0; x <= 1.0; x += 0.01 {
				require.GreaterOrEqual(t, curve(x), previous-1e-12, "never decreases at %g", x)
				previous = curve(x)
			}
			require.Equal(t, curve(0), curve(-5), "below the range clamps to the start")
			require.Equal(t, curve(1), curve(7), "above the range clamps to the end")
		})
	}
}

func TestCurve_RejectsBadParameters(t *testing.T) {
	tests := map[string]Dist{
		"unknown type":                {Type: "zipf"},
		"uniform with a parameter":    {Type: Uniform, Mean: f(1)},
		"gaussian with mu":            {Type: Gaussian, Mu: f(1)},
		"gaussian zero stddev":        {Type: Gaussian, StdDev: f(0)},
		"gaussian mean past the end":  {Type: Gaussian, Mean: f(2000)},
		"gaussian negative mean":      {Type: Gaussian, Mean: f(-1)},
		"lognormal missing sigma":     {Type: LogNormal, Mu: f(1)},
		"lognormal missing mu":        {Type: LogNormal, Sigma: f(1)},
		"lognormal zero sigma":        {Type: LogNormal, Mu: f(1), Sigma: f(0)},
		"lognormal with mean":         {Type: LogNormal, Mu: f(1), Sigma: f(1), Mean: f(1)},
		"lognormal mass out of range": {Type: LogNormal, Mu: f(20), Sigma: f(0.1)},
	}
	for name, d := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := d.Curve(1000)
			require.Error(t, err)
		})
	}
	_, err := Dist{}.Curve(0)
	require.Error(t, err, "a range of zero is rejected")
}

func TestApportion(t *testing.T) {
	t.Run("sums exactly to the total", func(t *testing.T) {
		counts, err := Apportion([]float64{1, 1, 1}, 100, 0)
		require.NoError(t, err)
		require.Equal(t, 100, sum(counts))
		require.Equal(t, []int{34, 33, 33}, counts, "the leftover goes to the lowest index on a tie")
	})

	t.Run("follows the shares", func(t *testing.T) {
		counts, err := Apportion([]float64{0.9, 0.1}, 1000, 0)
		require.NoError(t, err)
		require.Equal(t, []int{900, 100}, counts)
	})

	t.Run("shares need not sum to one", func(t *testing.T) {
		counts, err := Apportion([]float64{3, 1}, 8, 0)
		require.NoError(t, err)
		require.Equal(t, []int{6, 2}, counts)
	})

	t.Run("a minimum is given first and the rest apportioned", func(t *testing.T) {
		counts, err := Apportion([]float64{1000, 1, 0}, 100, 1)
		require.NoError(t, err)
		require.Equal(t, 100, sum(counts))
		for _, c := range counts {
			require.GreaterOrEqual(t, c, 1)
		}
	})

	t.Run("errors", func(t *testing.T) {
		_, err := Apportion([]float64{1, 1, 1}, 2, 1)
		require.Error(t, err, "total too small for the minimums")
		_, err = Apportion([]float64{0, 0}, 10, 0)
		require.Error(t, err, "all-zero shares")
		_, err = Apportion([]float64{1, -1}, 10, 0)
		require.Error(t, err, "negative share")
		counts, err := Apportion(nil, 10, 0)
		require.NoError(t, err)
		require.Empty(t, counts)
	})

	t.Run("is deterministic", func(t *testing.T) {
		shares := []float64{0.21, 0.33, 0.17, 0.29}
		first, _ := Apportion(shares, 1001, 0)
		for i := 0; i < 20; i++ {
			again, _ := Apportion(shares, 1001, 0)
			require.Equal(t, first, again)
		}
	})
}

func TestQueueCounts(t *testing.T) {
	const queues, total = 1000, 100000

	t.Run("uniform is flat", func(t *testing.T) {
		counts, err := QueueCounts(Dist{}, queues, total, 1)
		require.NoError(t, err)
		require.Equal(t, total, sum(counts))
		for _, c := range counts {
			require.Equal(t, 100, c)
		}
	})

	t.Run("gaussian is symmetric with its peak in the middle", func(t *testing.T) {
		counts, err := QueueCounts(Dist{Type: Gaussian}, queues, total, 1)
		require.NoError(t, err)
		require.Equal(t, total, sum(counts))
		require.InDelta(t, counts[249], counts[750], 2, "queue 250 and queue 751 mirror each other")
		require.Greater(t, counts[499], counts[249])
		require.Greater(t, counts[499], counts[0])
		require.GreaterOrEqual(t, counts[0], 1, "the minimum lifts the empty tails")
	})

	t.Run("lognormal peaks near the median and has a long right tail", func(t *testing.T) {
		d := Dist{Type: LogNormal, Mu: f(math.Log(100)), Sigma: f(1)}
		counts, err := QueueCounts(d, queues, total, 1)
		require.NoError(t, err)
		require.Equal(t, total, sum(counts))
		peak := 0
		for i, c := range counts {
			if c > counts[peak] {
				peak = i
			}
		}
		require.Less(t, peak+1, 100, "the mode of a lognormal is below its median")
		require.Greater(t, peak+1, 10)
		require.Greater(t, counts[299], counts[899], "and it decays to the right")
		for _, c := range counts {
			require.GreaterOrEqual(t, c, 1)
		}
	})

	t.Run("errors", func(t *testing.T) {
		_, err := QueueCounts(Dist{}, 0, 10, 0)
		require.Error(t, err)
		_, err = QueueCounts(Dist{}, 10, 5, 1)
		require.Error(t, err, "fewer jobs than the minimum needs")
		_, err = QueueCounts(Dist{Type: "zipf"}, 10, 100, 0)
		require.Error(t, err)
	})
}

func TestSchedulePlan(t *testing.T) {
	t.Run("duration zero is a single step at the start", func(t *testing.T) {
		plan, err := Schedule{Duration: 0}.Plan(500)
		require.NoError(t, err)
		require.Equal(t, []Step{{Offset: 0, Count: 500}}, plan)
	})

	t.Run("uniform spreads evenly and sums exactly", func(t *testing.T) {
		plan, err := Schedule{Duration: 10 * time.Minute, Step: 10 * time.Second}.Plan(10000)
		require.NoError(t, err)
		require.Len(t, plan, 60)
		total := 0
		for i, s := range plan {
			total += s.Count
			require.Equal(t, time.Duration(i)*10*time.Second, s.Offset)
			require.InDelta(t, 167, s.Count, 1)
		}
		require.Equal(t, 10000, total)
	})

	t.Run("gaussian peaks mid-run and is symmetric", func(t *testing.T) {
		plan, err := Schedule{Dist: Dist{Type: Gaussian}, Duration: 10 * time.Minute, Step: 10 * time.Second}.Plan(10000)
		require.NoError(t, err)
		total, peak := 0, 0
		for i, s := range plan {
			total += s.Count
			if s.Count > plan[peak].Count {
				peak = i
			}
		}
		require.Equal(t, 10000, total)
		require.InDelta(t, 30, peak, 1.5, "the busiest step is near the middle of 60")
		require.InDelta(t, plan[5].Count, plan[len(plan)-6].Count, 2)
		require.Less(t, plan[0].Count, 20)
	})

	t.Run("gaussian takes its mean and stddev in seconds", func(t *testing.T) {
		d := Dist{Type: Gaussian, Mean: f(120), StdDev: f(30)}
		plan, err := Schedule{Dist: d, Duration: 10 * time.Minute, Step: 10 * time.Second}.Plan(1000)
		require.NoError(t, err)
		peak := plan[0]
		for _, s := range plan {
			if s.Count > peak.Count {
				peak = s
			}
		}
		require.InDelta(t, 120, peak.Offset.Seconds(), 10, "the peak follows the mean, not the middle")
	})

	t.Run("a queue with few jobs skips the empty steps", func(t *testing.T) {
		plan, err := Schedule{Duration: 10 * time.Minute, Step: 10 * time.Second}.Plan(7)
		require.NoError(t, err)
		require.Len(t, plan, 7)
		for _, s := range plan {
			require.Equal(t, 1, s.Count)
		}
	})

	t.Run("zero jobs is an empty plan", func(t *testing.T) {
		plan, err := Schedule{Duration: time.Minute, Step: time.Second}.Plan(0)
		require.NoError(t, err)
		require.Empty(t, plan)
	})

	t.Run("a duration shorter than the step is one step", func(t *testing.T) {
		plan, err := Schedule{Duration: 3 * time.Second, Step: 10 * time.Second}.Plan(10)
		require.NoError(t, err)
		require.Equal(t, 10, sum(counts(plan)))
	})

	t.Run("errors", func(t *testing.T) {
		_, err := Schedule{Duration: -1}.Plan(10)
		require.Error(t, err, "continuous is not supported yet")
		_, err = Schedule{Dist: Dist{Type: LogNormal, Mu: f(1), Sigma: f(1)}, Duration: time.Minute, Step: time.Second}.Plan(10)
		require.Error(t, err, "lognormal is not available over time")
		_, err = Schedule{Duration: time.Minute}.Plan(10)
		require.Error(t, err, "a non-zero duration needs a step")
		_, err = Schedule{}.Plan(-1)
		require.Error(t, err)
	})
}

func counts(plan []Step) []int {
	out := make([]int, len(plan))
	for i, s := range plan {
		out[i] = s.Count
	}
	return out
}

func TestQueueShares(t *testing.T) {
	shares, err := QueueShares(Dist{Type: Gaussian}, 1000)
	require.NoError(t, err)
	require.Len(t, shares, 1000)
	total := 0.0
	for _, s := range shares {
		require.GreaterOrEqual(t, s, 0.0)
		total += s
	}
	require.InDelta(t, 1.0, total, 1e-9, "shares sum to one")
	require.InDelta(t, shares[249], shares[750], 1e-9, "a gaussian is symmetric")

	one, err := QueueShares(Dist{}, 1)
	require.NoError(t, err)
	require.Equal(t, []float64{1}, one)

	_, err = QueueShares(Dist{}, 0)
	require.Error(t, err)
}

func TestCarry(t *testing.T) {
	t.Run("a fractional rate emits a whole job every few steps and never loses the rest", func(t *testing.T) {
		c := NewCarry([]float64{0.3})
		total := 0
		for step := 0; step < 100; step++ {
			total += c.Next()[0]
		}
		require.Equal(t, 30, total, "100 steps at 0.3 is exactly 30 jobs")
	})

	t.Run("whole rates emit the same every step", func(t *testing.T) {
		c := NewCarry([]float64{2, 5})
		for i := 0; i < 3; i++ {
			require.Equal(t, []int{2, 5}, c.Next())
		}
	})

	t.Run("a small share still gets its jobs over time", func(t *testing.T) {
		// 1 job per step split 90/10: the 10% type must not starve behind the 90% one.
		c := NewCarry([]float64{0.9, 0.1})
		var big, small int
		for step := 0; step < 100; step++ {
			n := c.Next()
			big += n[0]
			small += n[1]
		}
		require.Equal(t, 90, big)
		require.Equal(t, 10, small)
	})

	t.Run("is deterministic and float error does not drop a job", func(t *testing.T) {
		total := 0
		c := NewCarry([]float64{0.1})
		for step := 0; step < 1000; step++ {
			total += c.Next()[0]
		}
		require.Equal(t, 100, total)
	})

	t.Run("does not alias the caller's slice", func(t *testing.T) {
		rates := []float64{1}
		c := NewCarry(rates)
		rates[0] = 100
		require.Equal(t, []int{1}, c.Next())
	})
}
