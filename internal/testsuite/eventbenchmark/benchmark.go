package eventbenchmark

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/armadaproject/armada/internal/common/context"

	"github.com/armadaproject/armada/pkg/api"
)

type EventsBenchmark struct {
	Out                   io.Writer
	c                     chan *api.EventMessage
	eventDurationsByJobId map[string]*EventDurationsByJobId
	mu                    sync.Mutex
}

func New(c chan *api.EventMessage) *EventsBenchmark {
	return &EventsBenchmark{
		Out: os.Stdout,
		c:   c,
	}
}

func (srv *EventsBenchmark) Run(ctx *context.ArmadaContext) error {
	srv.eventDurationsByJobId = make(map[string]*EventDurationsByJobId)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-srv.c: // Jobset event received.
			if e == nil {
				break
			}
			srv.recordEventDuration(e)
		}
	}
}

func (srv *EventsBenchmark) recordEventDuration(event *api.EventMessage) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	jobId := api.JobIdFromApiEvent(event)
	lastEventDuration := &EventDuration{Event: event.ShortString(), Received: time.Now()}
	if srv.eventDurationsByJobId[jobId] == nil {
		entry := &EventDurationsByJobId{JobId: jobId, Events: []*EventDuration{lastEventDuration}}
		srv.eventDurationsByJobId[jobId] = entry
	} else {
		srv.eventDurationsByJobId[jobId].Events = append(srv.eventDurationsByJobId[jobId].Events, lastEventDuration)
	}

	if len(srv.eventDurationsByJobId[jobId].Events) > 1 {
		index := len(srv.eventDurationsByJobId[jobId].Events) - 2
		srv.eventDurationsByJobId[jobId].Events[index].Duration = time.Since(lastEventDuration.Received)
	}
}

func (srv *EventsBenchmark) NewTestCaseBenchmarkReport(name string) *TestCaseBenchmarkReport {
	summary := make([]*EventDurationsByJobId, 0, len(srv.eventDurationsByJobId))
	for _, eventDurations := range srv.eventDurationsByJobId {
		summary = append(summary, eventDurations)
	}
	return &TestCaseBenchmarkReport{
		Name:       name,
		Statistics: calculateStatistics(summary),
		Summary:    summary,
	}
}

func calculateStatistics(summary []*EventDurationsByJobId) map[string]*Statistics {
	events := extractEventNames(summary)
	eventDurationsMap := newEventDurationsMap(summary, events)
	return newEventStatisticsMap(eventDurationsMap)
}

func extractEventNames(summary []*EventDurationsByJobId) []string {
	var eventNames []string
	for _, eventDurations := range summary {
		for _, e := range eventDurations.Events {
			if !in(eventNames, e.Event) {
				eventNames = append(eventNames, e.Event)
			}
		}
	}
	return eventNames
}

func newEventDurationsMap(summary []*EventDurationsByJobId, events []string) map[string][]*EventDuration {
	eventDurationsMap := make(map[string][]*EventDuration)
	for _, e := range events {
		eventDurationsMap[e] = extractEventDurationsForEvent(summary, e)
	}
	return eventDurationsMap
}

func extractEventDurationsForEvent(summary []*EventDurationsByJobId, event string) []*EventDuration {
	var durations []*EventDuration
	for _, eventDurations := range summary {
		for _, e := range eventDurations.Events {
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
		eventStatisticsMap[event] = statistics(eventDurationToInt64(durations))
	}
	return eventStatisticsMap
}
