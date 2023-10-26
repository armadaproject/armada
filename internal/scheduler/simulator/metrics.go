package simulator

import (
	"fmt"
	"strings"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type MetricsCollector struct {
	c              <-chan *armadaevents.EventSequence
	OverallMetrics MetricsVector
	MetricsByQueue map[string]MetricsVector
	// If non-zero, log a summary every this many events.
	LogSummaryInterval int
}

type MetricsVector struct {
	TimeOfMostRecentJobSucceededEvent time.Duration
	NumEvents                         int
	NumSubmitEvents                   int
	NumLeasedEvents                   int
	NumPreemptedEvents                int
	NumJobSucceededEvents             int
}

func NewMetricsCollector(c <-chan *armadaevents.EventSequence) *MetricsCollector {
	return &MetricsCollector{
		c:              c,
		MetricsByQueue: make(map[string]MetricsVector),
	}
}

func (mc *MetricsCollector) String() string {
	var sb strings.Builder
	sb.WriteString("{")
	sb.WriteString(fmt.Sprintf("Overall metrics: %s, Per-queue metrics: {", mc.OverallMetrics))
	i := 0
	queues := maps.Keys(mc.MetricsByQueue)
	slices.Sort(queues)
	for _, queue := range queues {
		metrics := mc.MetricsByQueue[queue]
		sb.WriteString(fmt.Sprintf("%s: %s", queue, metrics))
		i++
		if i != len(mc.MetricsByQueue) {
			sb.WriteString(", ")
		}
	}
	sb.WriteString("}}")
	return sb.String()
}

func (m MetricsVector) String() string {
	return fmt.Sprintf(
		"{FractionLeasedSucceeded: %f, TimeOfMostRecentJobSucceededEvent: %s, NumEvents: %d, NumPreemptedEvents: %d, NumLeasedEvents: %d, NumJobSucceededEvents: %d}",
		float64(m.NumJobSucceededEvents)/float64(m.NumLeasedEvents), m.TimeOfMostRecentJobSucceededEvent, m.NumEvents, m.NumPreemptedEvents, m.NumLeasedEvents, m.NumJobSucceededEvents,
	)
}

func (mc *MetricsCollector) Run(ctx *armadacontext.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case eventSequence, ok := <-mc.c:
			if !ok {
				return nil
			}
			mc.addEventSequence(eventSequence)
			if mc.LogSummaryInterval != 0 && mc.OverallMetrics.NumEvents%mc.LogSummaryInterval == 0 {
				ctx.Info(mc.String())
			}
		}
	}
}

func (mc *MetricsCollector) addEventSequence(eventSequence *armadaevents.EventSequence) {
	queue := eventSequence.Queue
	mc.OverallMetrics.NumEvents += 1
	perQueueMetrics := mc.MetricsByQueue[queue]
	perQueueMetrics.NumEvents += 1
	for _, event := range eventSequence.Events {
		switch event.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_SubmitJob:
			mc.OverallMetrics.NumSubmitEvents += 1
			perQueueMetrics.NumSubmitEvents += 1
		case *armadaevents.EventSequence_Event_JobRunLeased:
			mc.OverallMetrics.NumLeasedEvents += 1
			perQueueMetrics.NumLeasedEvents += 1
		case *armadaevents.EventSequence_Event_JobRunPreempted:
			mc.OverallMetrics.NumPreemptedEvents += 1
			perQueueMetrics.NumPreemptedEvents += 1
		case *armadaevents.EventSequence_Event_JobSucceeded:
			mc.OverallMetrics.TimeOfMostRecentJobSucceededEvent = event.Created.Sub(time.Time{})
			perQueueMetrics.TimeOfMostRecentJobSucceededEvent = event.Created.Sub(time.Time{})
			mc.OverallMetrics.NumJobSucceededEvents += 1
			perQueueMetrics.NumJobSucceededEvents += 1
		}
	}
	mc.MetricsByQueue[queue] = perQueueMetrics
}
