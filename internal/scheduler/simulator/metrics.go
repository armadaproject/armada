package simulator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

type MetricsCollector struct {
	c              <-chan *armadaevents.EventSequence
	Total          Metrics
	MetricsByQueue map[string]Metrics
	LastSeenEvent  *armadaevents.EventSequence_Event
}

func NewMetricsCollector(c <-chan *armadaevents.EventSequence) *MetricsCollector {
	return &MetricsCollector{
		c:              c,
		MetricsByQueue: make(map[string]Metrics),
	}
}

func (mc *MetricsCollector) String() string {
	var sb strings.Builder
	sb.WriteString("{")
	sb.WriteString(fmt.Sprintf("Total: %s, Queues: {", mc.Total))

	i := 0
	for queue, metrics := range mc.MetricsByQueue {
		sb.WriteString(fmt.Sprintf("%s: %s", queue, metrics))
		i++
		if i != len(mc.MetricsByQueue) {
			sb.WriteString(", ")
		}
	}
	sb.WriteString("}}")
	return sb.String()
}

type Metrics struct {
	LastJobSuccess      time.Duration
	NumEventsInTotal    int
	NumPreemptionEvents int
	NumSchedulingEvents int
	NumJobsSubmitted    int
	NumSchedules        int
	NumSuccesses        int
}

func (m Metrics) String() string {
	return fmt.Sprintf(
		"{LastJobSuccess: %s, NumEventsInTotal: %d, NumPreemptionEvents: %d, NumSchedulingEvents: %d}",
		m.LastJobSuccess, m.NumEventsInTotal, m.NumPreemptionEvents, m.NumSchedulingEvents,
	)
}

func (mc *MetricsCollector) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case eventSequence, ok := <-mc.c:
			if !ok {
				return nil
			}
			mc.addEventSequence(eventSequence)
			if mc.Total.NumEventsInTotal%500 == 0 {
				fmt.Println(mc.String())
			}
		}
	}
}

func (mc *MetricsCollector) addEventSequence(eventSequence *armadaevents.EventSequence) {
	queue := eventSequence.Queue
	mc.Total.NumEventsInTotal += 1
	entry := mc.MetricsByQueue[queue]
	entry.NumEventsInTotal += 1
	for _, event := range eventSequence.Events {
		mc.LastSeenEvent = event
		switch event.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_SubmitJob:
			mc.Total.NumJobsSubmitted += 1
			entry.NumJobsSubmitted += 1
		case *armadaevents.EventSequence_Event_JobRunLeased:
			mc.Total.NumSchedulingEvents += 1
			entry.NumSchedulingEvents += 1
		case *armadaevents.EventSequence_Event_JobRunPreempted:
			mc.Total.NumPreemptionEvents += 1
			entry.NumPreemptionEvents += 1
		case *armadaevents.EventSequence_Event_JobSucceeded:
			mc.Total.LastJobSuccess = event.Created.Sub(time.Time{})
			entry.LastJobSuccess = event.Created.Sub(time.Time{})
			mc.Total.NumSuccesses += 1
		}
	}
	mc.MetricsByQueue[queue] = entry
}
