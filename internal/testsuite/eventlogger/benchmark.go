package eventlogger

import (
	"fmt"
)

func (srv *EventsLogger) PrintBenchmarkReport() {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	fmt.Fprintf(srv.Out, "\nBenchmark:\n")
	report := srv.newBenchmarkReport()
	for _, s := range report.Summary {
		fmt.Fprintf(srv.Out, "%s:\n", s.JobId)
		for _, d := range s.Events {
			fmt.Fprintf(srv.Out, "\tname: %s, received: %s, duration: %s\n", d.Event, d.Received, d.Duration)
		}
	}
	fmt.Fprintf(srv.Out, "\nStatistics:\n")
	for event, stats := range report.Statistics {
		fmt.Fprintf(srv.Out, "\t* %s\n", event)
		fmt.Fprintf(srv.Out, "\t\t - min: %d\n", stats.Min)
		fmt.Fprintf(srv.Out, "\t\t - max: %d\n", stats.Max)
		fmt.Fprintf(srv.Out, "\t\t - avg: %f\n", stats.Average)
		fmt.Fprintf(srv.Out, "\t\t - variance: %f\n", stats.Variance)
		fmt.Fprintf(srv.Out, "\t\t - standard deviation: %f\n", stats.StandardDeviation)
	}
}

func (srv *EventsLogger) GenerateBenchmarkReport(formatter Formatter) ([]byte, error) {
	if formatter == nil {
		formatter = YamlFormatter
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return formatter(srv.newBenchmarkReport())
}

type BenchmarkReport struct {
	Statistics map[string]*Statistics   `json:"statistics"`
	Summary    []*EventDurationsByJobId `json:"summary"`
}

func (srv *EventsLogger) newBenchmarkReport() *BenchmarkReport {
	summary := make([]*EventDurationsByJobId, 0, len(srv.eventBenchmarksByJobId))
	for _, eventDurations := range srv.eventBenchmarksByJobId {
		summary = append(summary, eventDurations)
	}
	return &BenchmarkReport{
		Statistics: srv.calculateStatistics(),
		Summary:    summary,
	}
}

func (srv *EventsLogger) calculateStatistics() map[string]*Statistics {
	events := srv.extractEventNames()
	eventDurationsMap := srv.newEventDurationsMap(events)
	return newEventStatisticsMap(eventDurationsMap)
}

func (srv *EventsLogger) extractEventNames() []string {
	var eventNames []string
	for _, eb := range srv.eventBenchmarksByJobId {
		for _, e := range eb.Events {
			if !in(eventNames, e.Event) {
				eventNames = append(eventNames, e.Event)
			}
		}
	}
	return eventNames
}

func (srv *EventsLogger) newEventDurationsMap(events []string) map[string][]*EventDuration {
	eventDurationsMap := make(map[string][]*EventDuration)
	for _, e := range events {
		eventDurationsMap[e] = srv.extractEventDurationsForEvent(e)
	}
	return eventDurationsMap
}

func (srv *EventsLogger) extractEventDurationsForEvent(event string) []*EventDuration {
	var durations []*EventDuration
	for _, eb := range srv.eventBenchmarksByJobId {
		for _, e := range eb.Events {
			if e.Event == event {
				durations = append(durations, e)
			}
		}
	}
	return durations
}

func newEventStatisticsMap(eventDurationsMap map[string][]*EventDuration) map[string]*Statistics {
	eventStatisticsMap := make(map[string]*Statistics)
	for event, durations := range eventDurationsMap {
		eventStatisticsMap[event] = statistics(durations)
	}
	return eventStatisticsMap
}
