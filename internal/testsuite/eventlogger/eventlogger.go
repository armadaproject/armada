package eventlogger

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/armadaproject/armada/internal/common/context"

	"github.com/armadaproject/armada/pkg/api"
)

type EventLogger struct {
	// Destination to log output to.
	Out io.Writer
	// Channel on which to receive events to be logged.
	In chan *api.EventMessage
	// Interval at which to log output.
	interval time.Duration
	// For each job, the entire sequence of state transitions.
	transitionsByJobId map[string][]string
	// For each job, the state transitions that happened in the current interval.
	intervalTransitionsByJobId map[string][]string
	// Map from job id to the id of the job set the job is part of.
	jobSetIdByJobId map[string]string
	mu              sync.Mutex
}

func New(c chan *api.EventMessage, interval time.Duration) *EventLogger {
	return &EventLogger{
		Out:      os.Stdout,
		In:       c,
		interval: interval,
	}
}

func (srv *EventLogger) Log() {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	for transitions, counts := range countJobsByTransitions(srv.transitionsByJobId) {
		fmt.Fprintf(srv.Out, "%d:\t%s\n", counts, transitions)
	}
}

func (srv *EventLogger) Run(ctx *context.ArmadaContext) error {
	ticker := time.NewTicker(srv.interval)
	defer ticker.Stop()
	defer srv.flushAndLog()
	srv.transitionsByJobId = make(map[string][]string)
	srv.intervalTransitionsByJobId = make(map[string][]string)
	srv.jobSetIdByJobId = make(map[string]string)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C: // Print a summary of what happened in this interval.
			srv.flushAndLog()
		case e := <-srv.In: // Jobset event received.
			if e == nil {
				break
			}
			srv.mu.Lock()
			jobId := api.JobIdFromApiEvent(e)
			jobSet := api.JobSetIdFromApiEvent(e)
			srv.jobSetIdByJobId[jobId] = jobSet
			srv.intervalTransitionsByJobId[jobId] = append(srv.intervalTransitionsByJobId[jobId], e.ShortString())
			srv.mu.Unlock()
		}
	}
}

func (srv *EventLogger) flushAndLog() {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.intervalTransitionsByJobId) == 0 {
		return
	}
	bridgedTransitionsByJobId := bridgeIntervalTransitions(
		srv.intervalTransitionsByJobId,
		srv.transitionsByJobId,
	)
	intervalTransitionsByJobSetIdAndJobId := groupTransitionsByJobSetId(
		bridgedTransitionsByJobId,
		srv.jobSetIdByJobId,
	)

	// Create a tab writer, group, continue each group, print for each group
	w := tabwriter.NewWriter(srv.Out, 1, 1, 1, ' ', 0)
	for jobSetId, intervalTransitionsByJobId := range intervalTransitionsByJobSetIdAndJobId {
		for transitions, count := range countJobsByTransitions(intervalTransitionsByJobId) {
			fmt.Fprintf(w, "%d\t%s\t%s\n", count, jobSetId, transitions)
		}
	}
	fmt.Fprint(w, "---\n")
	w.Flush()

	// Move transitions over to the global map and reset the interval map.
	for jobId, transitions := range srv.intervalTransitionsByJobId {
		srv.transitionsByJobId[jobId] = append(srv.transitionsByJobId[jobId], transitions...)
	}
	srv.intervalTransitionsByJobId = make(map[string][]string)
}

// countJobsByTransitions returns a map from sequences of transitions,
// e.g., "submitted -> queued" to the number of jobs going through exactly those transitions.
func countJobsByTransitions(transitionsByJobId map[string][]string) map[string]int {
	numJobsFromEventSequence := make(map[string]int)
	for _, events := range transitionsByJobId {
		eventSequence := strings.Join(events, " -> ")
		numJobsFromEventSequence[eventSequence]++
	}
	return numJobsFromEventSequence
}

func groupTransitionsByJobSetId(
	transitionsByJobId map[string][]string,
	jobSetIdByJobId map[string]string,
) map[string]map[string][]string {
	rv := make(map[string]map[string][]string)
	for jobId, events := range transitionsByJobId {
		jobSetId := jobSetIdByJobId[jobId]
		if m := rv[jobSetId]; m != nil {
			m[jobId] = events
		} else {
			rv[jobSetId] = map[string][]string{jobId: events}
		}
	}
	return rv
}

func bridgeIntervalTransitions(
	intervalTransitionsByJobId map[string][]string,
	transitionsByJobId map[string][]string,
) map[string][]string {
	rv := make(map[string][]string)
	for jobId, transitions := range intervalTransitionsByJobId {
		if previousTransitions := transitionsByJobId[jobId]; len(previousTransitions) > 0 {
			previousState := previousTransitions[len(previousTransitions)-1]
			rv[jobId] = append([]string{previousState}, transitions...)
		} else {
			rv[jobId] = transitions
		}
	}
	return rv
}
