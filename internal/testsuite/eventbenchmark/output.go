package eventbenchmark

import (
	"fmt"
	"io"
)

func (r *TestBenchmarkReport) Print(out io.Writer) {
	_, _ = fmt.Fprintf(out, "\nBenchmark report %s:\n", r.Name)
	r.PrintSummary(out)
	r.PrintStatistics(out)
}

func (r *TestBenchmarkReport) PrintSummary(out io.Writer) {
	_, _ = fmt.Fprintf(out, "\nSummary:\n")
	for _, s := range r.Summary {
		_, _ = fmt.Fprintf(out, "%s:\n", s.JobId)
		for _, d := range s.Events {
			_, _ = fmt.Fprintf(out, "\tname: %s, received: %s, duration: %s\n", d.Event, d.Received, d.Duration)
		}
	}
}

func (r *TestBenchmarkReport) PrintStatistics(out io.Writer) {
	_, _ = fmt.Fprintf(out, "\nStatistics:\n")
	for event, stats := range r.Statistics {
		_, _ = fmt.Fprintf(out, "\t* %s\n", event)
		_, _ = fmt.Fprintf(out, "\t\t - min: %d\n", stats.Min)
		_, _ = fmt.Fprintf(out, "\t\t - max: %d\n", stats.Max)
		_, _ = fmt.Fprintf(out, "\t\t - avg: %f\n", stats.Average)
		_, _ = fmt.Fprintf(out, "\t\t - variance: %f\n", stats.Variance)
		_, _ = fmt.Fprintf(out, "\t\t - standard deviation: %f\n", stats.StandardDeviation)
	}
}

func (r *TestBenchmarkReport) Generate(formatter Formatter) ([]byte, error) {
	if formatter == nil {
		formatter = YamlFormatter
	}
	return formatter(r)
}
