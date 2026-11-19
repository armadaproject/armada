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

type jobComparison struct {
	ID       string `json:"logical_job_id"`
	Queue    string `json:"queue"`
	Priority string `json:"priority_class"`
	Delay    metric `json:"first_allocation_delay_us"`
	StateA   State  `json:"state_a"`
	StateB   State  `json:"state_b"`
}

type groupComparison struct {
	Queue           string `json:"queue"`
	Priority        string `json:"priority_class"`
	Arrivals        metric `json:"arrivals"`
	CarryIn         metric `json:"carry_in"`
	Allocated       metric `json:"allocated_arrivals"`
	Unallocated     metric `json:"unallocated_arrivals"`
	Completed       metric `json:"completed"`
	QueuedAtCutoff  metric `json:"queued_at_cutoff"`
	RunningAtCutoff metric `json:"running_at_cutoff"`
}

type comparisonReport struct {
	Version     int               `json:"version"`
	Change      string            `json:"change"`
	Code        *codeChange       `json:"code,omitempty"`
	Profile     string            `json:"profile"`
	Model       string            `json:"model"`
	Window      Window            `json:"window"`
	Allocated   metric            `json:"allocated_cpu_seconds"`
	Available   metric            `json:"available_cpu_seconds"`
	Utilisation metric            `json:"cpu_utilisation"`
	Groups      []groupComparison `json:"groups"`
	Jobs        []jobComparison   `json:"jobs"`
}

type groupKey struct{ queue, priority string }

type counts struct {
	arrivals, carryIn, allocated, completed, queued, running float64
}

func compareResults(change string, code *codeChange, a, b *runResult) comparisonReport {
	report := comparisonReport{
		Version: version, Change: change, Code: code, Profile: a.Profile, Model: a.Model, Window: a.Window,
		Allocated:   difference(a.AllocatedCPUSeconds, b.AllocatedCPUSeconds),
		Available:   difference(a.AvailableCPUSeconds, b.AvailableCPUSeconds),
		Utilisation: difference(utilisation(a), utilisation(b)),
		Groups:      []groupComparison{}, Jobs: []jobComparison{},
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
		report.Groups = append(report.Groups, groupComparison{
			Queue: key.queue, Priority: key.priority, Arrivals: values(x.arrivals, y.arrivals),
			CarryIn: values(x.carryIn, y.carryIn), Allocated: values(x.allocated, y.allocated),
			Unallocated: values(x.arrivals-x.allocated, y.arrivals-y.allocated),
			Completed:   values(x.completed, y.completed), QueuedAtCutoff: values(x.queued, y.queued),
			RunningAtCutoff: values(x.running, y.running),
		})
	}
	slices.SortFunc(report.Groups, func(x, y groupComparison) int {
		if x.Queue != y.Queue {
			return strings.Compare(x.Queue, y.Queue)
		}
		return strings.Compare(x.Priority, y.Priority)
	})
	for i, x := range a.Outcomes {
		y := b.Outcomes[i]
		if x.SubmittedUS >= a.Window.StartUS {
			report.Jobs = append(report.Jobs, jobComparison{
				ID: x.ID, Queue: x.Queue, Priority: x.Priority, StateA: x.State, StateB: y.State,
				Delay: difference(delay(x), delay(y)),
			})
		}
	}
	return report
}

func countGroups(run *runResult) map[groupKey]counts {
	result := make(map[groupKey]counts)
	for _, row := range run.Outcomes {
		if row.ObservedUS < run.Window.StartUS {
			continue
		}
		key := groupKey{row.Queue, row.Priority}
		count := result[key]
		if row.SubmittedUS >= run.Window.StartUS {
			count.arrivals++
			if row.FirstAllocationUS != nil {
				count.allocated++
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
	return result
}

func delay(row outcome) *float64 {
	if row.FirstAllocationUS == nil {
		return nil
	}
	// The replay window is bounded below 2^53 microseconds.
	delayUS := float64(*row.FirstAllocationUS - row.SubmittedUS)
	return &delayUS
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
	row := func(label string, m metric) {
		line("%s\t%s\t%s\t%s\n", label, display(m.A), display(m.B), display(m.Delta))
	}
	line("Change %s\n", r.Change)
	if r.Code != nil {
		line("Declared code change %q in %q\n", r.Code.Behavior, r.Code.Path)
	}
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
	}
	if len(r.Jobs) > 0 {
		line("\nFirst-allocation delay (µs)\tA\tB\tB−A\n")
		for _, job := range r.Jobs {
			row(strconv.Quote(job.ID), job.Delay)
		}
	}
	return errors.Join(err, w.Flush())
}

func display(v *float64) string {
	if v == nil {
		return "—"
	}
	return strconv.FormatFloat(*v, 'f', -1, 64)
}
