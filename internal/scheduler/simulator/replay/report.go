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

type metricValue interface{ int64 | float64 }

type metric[T metricValue] struct {
	A     *T `json:"a"`
	B     *T `json:"b"`
	Delta *T `json:"delta"`
}

func values(a, b float64) metric[float64] {
	return difference(&a, &b)
}

func difference[T metricValue](a, b *T) metric[T] {
	m := metric[T]{A: a, B: b}
	if a != nil && b != nil {
		delta := *b - *a
		m.Delta = &delta
	}
	return m
}

const comparisonReportVersion = 1

type waitPercentiles struct {
	P50 metric[int64] `json:"p50"`
	P95 metric[int64] `json:"p95"`
	P99 metric[int64] `json:"p99"`
}

type groupComparison struct {
	Queue                      string          `json:"queue"`
	Priority                   string          `json:"priority_class"`
	Arrivals                   metric[float64] `json:"arrivals"`
	CarryIn                    metric[float64] `json:"carry_in"`
	Allocated                  metric[float64] `json:"allocated_arrivals"`
	Unallocated                metric[float64] `json:"unallocated_arrivals"`
	Completed                  metric[float64] `json:"completed"`
	QueuedAtCutoff             metric[float64] `json:"queued_at_cutoff"`
	RunningAtCutoff            metric[float64] `json:"running_at_cutoff"`
	FirstAllocationWaitSeconds waitPercentiles `json:"first_allocation_wait_s"`
}

type comparisonReport struct {
	Version int    `json:"version"`
	Change  string `json:"change"`

	Window      Window            `json:"window"`
	Allocated   metric[float64]   `json:"allocated_cpu_seconds"`
	Available   metric[float64]   `json:"available_cpu_seconds"`
	Utilisation metric[float64]   `json:"cpu_utilisation"`
	Groups      []groupComparison `json:"groups"`
}

type groupKey struct{ queue, priority string }

type counts struct {
	arrivals, carryIn, completed, queued, running float64
	waitsSeconds                                  []int64
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
		allocatedA, allocatedB := float64(len(x.waitsSeconds)), float64(len(y.waitsSeconds))
		report.Groups = append(report.Groups, groupComparison{
			Queue: key.queue, Priority: key.priority, Arrivals: values(x.arrivals, y.arrivals),
			CarryIn: values(x.carryIn, y.carryIn), Allocated: values(allocatedA, allocatedB),
			Unallocated: values(x.arrivals-allocatedA, y.arrivals-allocatedB),
			Completed:   values(x.completed, y.completed), QueuedAtCutoff: values(x.queued, y.queued),
			RunningAtCutoff: values(x.running, y.running),
			FirstAllocationWaitSeconds: waitPercentiles{
				P50: difference(percentile(x.waitsSeconds, 50), percentile(y.waitsSeconds, 50)),
				P95: difference(percentile(x.waitsSeconds, 95), percentile(y.waitsSeconds, 95)),
				P99: difference(percentile(x.waitsSeconds, 99), percentile(y.waitsSeconds, 99)),
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
			if row.FirstAllocationSeconds != nil {
				count.waitsSeconds = append(count.waitsSeconds, *row.FirstAllocationSeconds-row.SubmittedSeconds)
			}
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
		slices.Sort(group.waitsSeconds)
	}
	return result
}

// percentile uses the nearest rank in the sorted, non-interpolated sample.
func percentile(sorted []int64, p int) *int64 {
	if len(sorted) == 0 {
		return nil
	}
	rank := (len(sorted)*p + 99) / 100
	return &sorted[rank-1]
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

func (m metric[T]) columns() [3]string {
	return [3]string{display(m.A), display(m.B), display(m.Delta)}
}

func display[T metricValue](v *T) string {
	if v == nil {
		return "—"
	}
	switch value := any(*v).(type) {
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64)
	default:
		return fmt.Sprint(value)
	}
}
