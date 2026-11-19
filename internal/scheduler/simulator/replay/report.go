package replay

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
)

type metric struct {
	A     *float64 `json:"a"`
	B     *float64 `json:"b"`
	Delta *float64 `json:"delta"`
}

func values(a, b float64) metric {
	return difference(&a, &b)
}

func difference(a, b *float64) metric {
	m := metric{A: a, B: b}
	if a != nil && b != nil {
		delta := *b - *a
		m.Delta = &delta
	}
	return m
}

const comparisonReportVersion = 1

type waitBounds struct {
	Lower int64  `json:"lower"`
	Upper *int64 `json:"upper"`
}

type waitComparison struct {
	A     *waitBounds `json:"a"`
	B     *waitBounds `json:"b"`
	Delta *int64      `json:"delta"`
}

type waitPercentiles struct {
	P50 waitComparison `json:"p50"`
	P95 waitComparison `json:"p95"`
	P99 waitComparison `json:"p99"`
}

type groupComparison struct {
	Queue                      string          `json:"queue"`
	Priority                   string          `json:"priority_class"`
	Arrivals                   metric          `json:"arrivals"`
	CarryIn                    metric          `json:"carry_in"`
	Allocated                  metric          `json:"allocated_arrivals"`
	Unallocated                metric          `json:"unallocated_arrivals"`
	Completed                  metric          `json:"completed"`
	QueuedAtCutoff             metric          `json:"queued_at_cutoff"`
	RunningAtCutoff            metric          `json:"running_at_cutoff"`
	FirstAllocationWaitSeconds waitPercentiles `json:"first_allocation_wait_s"`
}

type comparisonReport struct {
	Version      int    `json:"version"`
	Change       string `json:"change"`
	BinarySHA256 string `json:"binary_sha256"`

	Window      Window            `json:"window"`
	Allocated   metric            `json:"allocated_cpu_seconds"`
	Available   metric            `json:"available_cpu_seconds"`
	Utilisation metric            `json:"cpu_utilisation"`
	Groups      []groupComparison `json:"groups"`
}

type groupKey struct{ queue, priority string }

type counts struct {
	arrivals, carryIn, completed, queued, running float64
	waitLowerSeconds                              []int64
	allocatedWaitSeconds                          []int64
}

func compareResults(change string, a, b *runResult) comparisonReport {
	report := comparisonReport{
		Version: comparisonReportVersion, Change: change, Window: a.Window,
		Allocated:   difference(a.AllocatedCPUSeconds, b.AllocatedCPUSeconds),
		Available:   difference(a.AvailableCPUSeconds, b.AvailableCPUSeconds),
		Utilisation: difference(utilisation(a), utilisation(b)),
		Groups:      []groupComparison{},
	}
	left, right := countGroups(a), countGroups(b)
	keys := make(map[groupKey]bool)
	for key := range left {
		keys[key] = true
	}
	for key := range right {
		keys[key] = true
	}
	for key := range keys {
		x, y := left[key], right[key]
		allocatedA, allocatedB := float64(len(x.allocatedWaitSeconds)), float64(len(y.allocatedWaitSeconds))
		report.Groups = append(report.Groups, groupComparison{
			Queue: key.queue, Priority: key.priority, Arrivals: values(x.arrivals, y.arrivals),
			CarryIn: values(x.carryIn, y.carryIn), Allocated: values(allocatedA, allocatedB),
			Unallocated: values(x.arrivals-allocatedA, y.arrivals-allocatedB),
			Completed:   values(x.completed, y.completed), QueuedAtCutoff: values(x.queued, y.queued),
			RunningAtCutoff: values(x.running, y.running),
			FirstAllocationWaitSeconds: waitPercentiles{
				P50: compareWaitPercentile(x, y, 50),
				P95: compareWaitPercentile(x, y, 95),
				P99: compareWaitPercentile(x, y, 99),
			},
		})
	}
	slices.SortFunc(report.Groups, func(x, y groupComparison) int {
		if x.Queue != y.Queue {
			return strings.Compare(x.Queue, y.Queue)
		}
		return strings.Compare(x.Priority, y.Priority)
	})
	return report
}

func countGroups(run *runResult) map[groupKey]counts {
	result := make(map[groupKey]counts)
	for _, row := range run.Outcomes {
		if row.ObservedSeconds < run.Window.StartSeconds {
			continue
		}
		key := groupKey{row.Queue, row.Priority}
		count := result[key]
		if row.SubmittedSeconds >= run.Window.StartSeconds {
			count.arrivals++
			wait := run.Window.EndSeconds - row.SubmittedSeconds
			if row.FirstAllocationSeconds != nil {
				wait = *row.FirstAllocationSeconds - row.SubmittedSeconds
				count.allocatedWaitSeconds = append(count.allocatedWaitSeconds, wait)
			}
			count.waitLowerSeconds = append(count.waitLowerSeconds, wait)
		} else {
			count.carryIn++
		}
		switch row.State {
		case Completed:
			count.completed++
		case Queued:
			count.queued++
		case Running:
			count.running++
		}
		result[key] = count
	}
	for _, group := range result {
		slices.Sort(group.waitLowerSeconds)
		slices.Sort(group.allocatedWaitSeconds)
	}
	return result
}

// waitPercentile uses the nearest rank over all arrivals. Substituting the
// cutoff age for each censored wait gives the lower bound. Treating each
// censored wait as unbounded gives the upper bound, which may be unknown.
func (c counts) waitPercentile(p int) *waitBounds {
	if len(c.waitLowerSeconds) == 0 {
		return nil
	}
	rank := (len(c.waitLowerSeconds)*p + 99) / 100
	bounds := &waitBounds{Lower: c.waitLowerSeconds[rank-1]}
	if rank <= len(c.allocatedWaitSeconds) {
		bounds.Upper = &c.allocatedWaitSeconds[rank-1]
	}
	return bounds
}

func compareWaitPercentile(a, b counts, p int) waitComparison {
	left, right := a.waitPercentile(p), b.waitPercentile(p)
	result := waitComparison{A: left, B: right}
	if left.exact() && right.exact() {
		delta := right.Lower - left.Lower
		result.Delta = &delta
	}
	return result
}

func (w *waitBounds) exact() bool {
	return w != nil && w.Upper != nil && w.Lower == *w.Upper
}

func utilisation(r *runResult) *float64 {
	if *r.AvailableCPUSeconds == 0 {
		return nil
	}
	ratio := *r.AllocatedCPUSeconds / *r.AvailableCPUSeconds
	return &ratio
}

func (r comparisonReport) print(out io.Writer) error {
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	var err error
	line := func(format string, args ...any) {
		if err == nil {
			_, err = fmt.Fprintf(w, format, args...)
		}
	}
	row := func(label string, m interface{ columns() [3]string }) {
		values := m.columns()
		line("%s\t%s\t%s\t%s\n", label, values[0], values[1], values[2])
	}
	line("Change %s\n", r.Change)
	line("Metric\tA\tB\tB−A\n")
	row("Allocated CPU-seconds", r.Allocated)
	row("Available CPU-seconds", r.Available)
	row("CPU utilisation (fraction)", r.Utilisation)
	for _, g := range r.Groups {
		line("\nQueue %q / priority %q\n", g.Queue, g.Priority)
		row("Arrivals", g.Arrivals)
		row("Carry-in", g.CarryIn)
		row("Allocated arrivals", g.Allocated)
		row("Unallocated arrivals", g.Unallocated)
		row("Completed", g.Completed)
		row("Queued at cutoff", g.QueuedAtCutoff)
		row("Running at cutoff", g.RunningAtCutoff)
		row("First-allocation wait p50 (s)", g.FirstAllocationWaitSeconds.P50)
		row("First-allocation wait p95 (s)", g.FirstAllocationWaitSeconds.P95)
		row("First-allocation wait p99 (s)", g.FirstAllocationWaitSeconds.P99)
	}
	return errors.Join(err, w.Flush())
}

func (m metric) columns() [3]string {
	return [3]string{display(m.A), display(m.B), display(m.Delta)}
}

func display(v *float64) string {
	if v == nil {
		return "—"
	}
	return strconv.FormatFloat(*v, 'f', -1, 64)
}

func (w waitComparison) columns() [3]string {
	delta := "—"
	if w.Delta != nil {
		delta = strconv.FormatInt(*w.Delta, 10)
	}
	return [3]string{w.A.display(), w.B.display(), delta}
}

func (w *waitBounds) display() string {
	switch {
	case w == nil:
		return "—"
	case w.exact():
		return strconv.FormatInt(w.Lower, 10)
	case w.Upper == nil:
		return fmt.Sprintf("≥ %d", w.Lower)
	default:
		return fmt.Sprintf("%d–%d", w.Lower, *w.Upper)
	}
}
