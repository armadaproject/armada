package eventlogger

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/G-Research/armada/pkg/api"
)

type EventsLogger struct {
	Out                        io.Writer
	c                          chan *api.EventMessage
	interval                   time.Duration
	eventBenchmarksByJobId     map[string]*EventDurationsByJobId
	transitionsByJobId         map[string][]string
	intervalTransitionsByJobId map[string][]string
	mu                         sync.Mutex
}

func New(c chan *api.EventMessage, interval time.Duration) *EventsLogger {
	return &EventsLogger{
		Out:      os.Stdout,
		c:        c,
		interval: interval,
	}
}

type EventDurationsByJobId struct {
	JobId  string           `json:"jobId"`
	Events []*EventDuration `json:"events"`
}

type EventDuration struct {
	Received time.Time     `json:"received"`
	Duration time.Duration `json:"duration"`
	Event    string        `json:"event"`
}

func (srv *EventsLogger) flushAndLog() {
	if len(srv.intervalTransitionsByJobId) == 0 {
		return
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()

	// For each job for which we already have some state transitions,
	// add the most recent state to the state transitions seen in this interval.
	// This makes it more clear what's going on.
	continuedTransitionsByJobId := make(map[string][]string)
	for jobId, transitions := range srv.intervalTransitionsByJobId {
		if previousTransitions := srv.transitionsByJobId[jobId]; len(previousTransitions) > 0 {
			previousState := previousTransitions[len(previousTransitions)-1]
			continuedTransitionsByJobId[jobId] = append([]string{previousState}, transitions...)
		} else {
			continuedTransitionsByJobId[jobId] = transitions
		}
	}

	// Print the number of jobs for each unique sequence of state transitions.
	for transitions, counts := range CountJobsByTransitions(continuedTransitionsByJobId) {
		fmt.Fprintf(srv.Out, "%d:\t%s\n", counts, transitions)
	}
	fmt.Fprintf(srv.Out, "\n") // Indicates the end of the interval.

	// Move transitions over to the global map and reset the interval map.
	for jobId, transitions := range srv.intervalTransitionsByJobId {
		srv.transitionsByJobId[jobId] = append(srv.transitionsByJobId[jobId], transitions...)
	}
	srv.intervalTransitionsByJobId = make(map[string][]string)
}

func (srv *EventsLogger) Log() {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	for transitions, counts := range CountJobsByTransitions(srv.transitionsByJobId) {
		fmt.Fprintf(srv.Out, "%d:\t%s\n", counts, transitions)
	}
}

// CountJobsByTransitions returns a map from sequences of transitions,
// e.g., "submitted -> queued" to the number of jobs going through exactly those transitions.
func CountJobsByTransitions(transitionsByJobId map[string][]string) map[string]int {
	numJobsFromEventSequence := make(map[string]int)
	for _, events := range transitionsByJobId {
		eventSequence := strings.Join(events, " -> ")
		numJobsFromEventSequence[eventSequence]++
	}
	return numJobsFromEventSequence
}

func (srv *EventsLogger) Run(ctx context.Context) error {
	ticker := time.NewTicker(srv.interval)
	defer ticker.Stop()
	defer srv.flushAndLog()
	srv.transitionsByJobId = make(map[string][]string)
	srv.intervalTransitionsByJobId = make(map[string][]string)
	srv.eventBenchmarksByJobId = make(map[string]*EventDurationsByJobId)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C: // Print a summary of what happened in this interval.
			srv.flushAndLog()
		case e := <-srv.c: // Jobset event received.
			if e == nil {
				break
			}
			jobId := api.JobIdFromApiEvent(e)
			s := shortStringFromApiEvent(e)
			srv.intervalTransitionsByJobId[jobId] = append(srv.intervalTransitionsByJobId[jobId], s)
			srv.recordEventDuration(e)
		}
	}
}

func (srv *EventsLogger) recordEventDuration(event *api.EventMessage) {
	jobId := api.JobIdFromApiEvent(event)
	shortName := shortStringFromApiEvent(event)
	lastEventDuration := &EventDuration{Event: shortName, Received: time.Now()}
	if srv.eventBenchmarksByJobId[jobId] == nil {
		entry := &EventDurationsByJobId{JobId: jobId, Events: []*EventDuration{lastEventDuration}}
		srv.eventBenchmarksByJobId[jobId] = entry
	} else {
		srv.eventBenchmarksByJobId[jobId].Events = append(srv.eventBenchmarksByJobId[jobId].Events, lastEventDuration)
	}

	if len(srv.eventBenchmarksByJobId[jobId].Events) > 1 {
		index := len(srv.eventBenchmarksByJobId[jobId].Events) - 2
		srv.eventBenchmarksByJobId[jobId].Events[index].Duration = time.Since(lastEventDuration.Received)
	}
}

func shortStringFromApiEvent(msg *api.EventMessage) string {
	s := stringFromApiEvent(msg)
	s = strings.ReplaceAll(s, "*api.EventMessage_", "")
	return s
}

func stringFromApiEvent(msg *api.EventMessage) string {
	return fmt.Sprintf("%T", msg.Events)
}
