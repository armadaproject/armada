package startup

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/executor/configuration"
)

// markerConfig is aliased only to keep the tables below readable - the literals are otherwise long
// enough to wrap every row.
type markerConfig = configuration.StartUpMarkerConfig

func stepConfig(name string, start, end *markerConfig) configuration.StartUpStepConfig {
	return configuration.StartUpStepConfig{Name: name, Start: start, End: end}
}

func timingsWithSteps(t *testing.T, steps ...configuration.StartUpStepConfig) *Timings {
	t.Helper()
	timings, err := NewTimings(configuration.StartUpTimingsConfig{ExtraSteps: steps})
	require.NoError(t, err)
	return timings
}

// multusInterfaceEvents reproduces what multus records while configuring a pod's interfaces: one
// AddedInterface event per interface, all sharing a reason and a source, distinguishable only by
// message. The count varies by pool, which is why a step cannot name the last one.
func multusInterfaceEvents(ibInterfaces int) []*v1.Event {
	events := []*v1.Event{{
		Reason:         "AddedInterface",
		Message:        "Add eth0 [10.0.0.1/32] from cilium",
		Source:         v1.EventSource{Component: "multus"},
		FirstTimestamp: baselineWithOffset(0),
	}}
	for i := 1; i <= ibInterfaces; i++ {
		events = append(events, &v1.Event{
			Reason:         "AddedInterface",
			Message:        fmt.Sprintf("Add net%d [] from test-ns/ib-sriov-network-%d", i, i),
			Source:         v1.EventSource{Component: "multus"},
			FirstTimestamp: baselineWithOffset(i),
		})
	}
	return events
}

// Config that could never work has to fail the executor at startup. A step that silently never
// matches is indistinguishable from the plugin not emitting its events, so the mistake would only
// surface as permanently missing data.
func TestNewTimings_RejectsUnworkableConfig(t *testing.T) {
	marker := func(reason string) *markerConfig { return &markerConfig{Reason: reason} }

	tests := map[string]struct {
		steps          []configuration.StartUpStepConfig
		expectedInText string
	}{
		"invalid regexp": {
			steps:          []configuration.StartUpStepConfig{stepConfig("bad", &markerConfig{Message: "("}, marker("Done"))},
			expectedInText: "bad",
		},
		// A marker with neither reason nor message would match the first event of any kind,
		// producing a plausible-looking but meaningless duration.
		"marker with no criteria": {
			steps:          []configuration.StartUpStepConfig{stepConfig("empty", &markerConfig{}, marker("Done"))},
			expectedInText: "empty",
		},
		"unknown match mode": {
			steps:          []configuration.StartUpStepConfig{stepConfig("bad-match", marker("A"), &markerConfig{Reason: "B", Match: "latest"})},
			expectedInText: "latest",
		},
		"no name":  {steps: []configuration.StartUpStepConfig{stepConfig("", marker("A"), marker("B"))}},
		"no start": {steps: []configuration.StartUpStepConfig{stepConfig("step", nil, marker("B"))}},
		"no end":   {steps: []configuration.StartUpStepConfig{stepConfig("step", marker("A"), nil)}},
		// A step name identifies the step in the event and is destined to be a metric label, so a
		// repeat would silently fold two different measurements together.
		"duplicate names": {
			steps: []configuration.StartUpStepConfig{
				stepConfig("cni", marker("A"), marker("B")),
				stepConfig("cni", marker("C"), marker("D")),
			},
			expectedInText: "more than once",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := NewTimings(configuration.StartUpTimingsConfig{ExtraSteps: tc.steps})

			require.Error(t, err)
			if tc.expectedInText != "" {
				assert.Contains(t, err.Error(), tc.expectedInText)
			}
		})
	}
}

// Every case is the same shape: a configured step and a set of the pod's events produce either one
// measured step or none at all.
//
// The cases that report nothing are as important as the ones that measure. Both markers are
// required, because a start marker is often a convenient earlier anchor rather than a signal that
// the step itself began - "the first interface was added" says nothing about whether SR-IOV was
// configured - so a half-matched step would report a meaningless instant on every pod the step does
// not apply to.
func TestMeasure_ExtraSteps(t *testing.T) {
	addedInterface := &markerConfig{Reason: "^AddedInterface$"}
	lastAddedInterface := &markerConfig{Reason: "^AddedInterface$", Match: "last"}
	srIovInterface := &markerConfig{Reason: "^AddedInterface$", Message: "ib-sriov-network"}
	lastSrIovInterface := &markerConfig{Reason: "^AddedInterface$", Message: "ib-sriov-network", Match: "last"}

	tests := map[string]struct {
		step   configuration.StartUpStepConfig
		events []*v1.Event
		// A nil expectedStart means the step is expected not to be reported at all.
		expectedStart *time.Time
		expectedEnd   *time.Time
	}{
		"between two matched reasons": {
			step: stepConfig("infinibandConfiguration", &markerConfig{Reason: "IBConfiguring"}, &markerConfig{Reason: "IBConfigured"}),
			// Appended to a real pod's kubelet events, so unrelated traffic must not match.
			events: append(startedUpPodEvents(),
				&v1.Event{Reason: "IBConfiguring", Message: "Binding 8 IB devices", FirstTimestamp: baselineWithOffset(10)},
				&v1.Event{Reason: "IBConfigured", Message: "Configured 8 IB devices", FirstTimestamp: baselineWithOffset(13)},
			),
			expectedStart: new(baselineWithOffset(10).Time),
			expectedEnd:   new(baselineWithOffset(13).Time),
		},
		// Reason is a regex too, so a plugin that varies its reason per phase or per device can be
		// matched without enumerating every value.
		"reason matched as a regex": {
			step: stepConfig("ibSetup", &markerConfig{Reason: "^IBConfigur"}, &markerConfig{Reason: "^IBConfigured$"}),
			events: []*v1.Event{
				{Reason: "IBConfiguring", Message: "binding", FirstTimestamp: baselineWithOffset(5)},
				{Reason: "IBConfigured", Message: "done", FirstTimestamp: baselineWithOffset(11)},
			},
			expectedStart: new(baselineWithOffset(5).Time),
			expectedEnd:   new(baselineWithOffset(11).Time),
		},
		// Matching on the message is the point of the message regex: a plugin that reports its
		// phases through one event reason can still be split into steps.
		"message pattern splits one reason": {
			step: stepConfig("deviceBinding",
				&markerConfig{Reason: "Plugin", Message: `^binding devices`},
				&markerConfig{Reason: "Plugin", Message: `^bound \d+ devices`}),
			events: []*v1.Event{
				{Reason: "Plugin", Message: "binding devices for pod", FirstTimestamp: baselineWithOffset(5)},
				{Reason: "Plugin", Message: "bound 8 devices", FirstTimestamp: baselineWithOffset(11)},
			},
			expectedStart: new(baselineWithOffset(5).Time),
			expectedEnd:   new(baselineWithOffset(11).Time),
		},
		// match: last spans a run of events sharing one reason without the config knowing how many
		// there are, so one global config works on a pool with four interfaces and one with two.
		"match last spans four interfaces": {
			step:          stepConfig("additionalInterfaces", addedInterface, lastAddedInterface),
			events:        multusInterfaceEvents(4),
			expectedStart: new(baselineWithOffset(0).Time),
			expectedEnd:   new(baselineWithOffset(4).Time),
		},
		"match last spans two interfaces": {
			step:          stepConfig("additionalInterfaces", addedInterface, lastAddedInterface),
			events:        multusInterfaceEvents(2),
			expectedStart: new(baselineWithOffset(0).Time),
			expectedEnd:   new(baselineWithOffset(2).Time),
		},
		// A single matching event makes match: last pick that same event, so the step is a point in
		// time rather than being dropped.
		"match last with only one match": {
			step:          stepConfig("additionalInterfaces", addedInterface, lastAddedInterface),
			events:        multusInterfaceEvents(0),
			expectedStart: new(baselineWithOffset(0).Time),
			expectedEnd:   new(baselineWithOffset(0).Time),
		},
		// An unset match behaves as first, which is what a step naming two distinct markers wants.
		"unset match defaults to first": {
			step:          stepConfig("additionalInterfaces", addedInterface, addedInterface),
			events:        multusInterfaceEvents(4),
			expectedStart: new(baselineWithOffset(0).Time),
			expectedEnd:   new(baselineWithOffset(0).Time),
		},
		// Scoping both markers to the SR-IOV networks omits the step on a pod that has none, rather
		// than reporting it as having taken no time - at the cost of the first interface's own
		// attach time falling outside the span.
		"scoped to the sr-iov networks": {
			step:          stepConfig("infinibandConfiguration", srIovInterface, lastSrIovInterface),
			events:        multusInterfaceEvents(4),
			expectedStart: new(baselineWithOffset(1).Time),
			expectedEnd:   new(baselineWithOffset(4).Time),
		},
		"scoped to the sr-iov networks, pod has none": {
			step:   stepConfig("infinibandConfiguration", srIovInterface, lastSrIovInterface),
			events: multusInterfaceEvents(0),
		},
		// A generic start with a specific end measures the whole span including the first SR-IOV
		// interface's own attach time, since eth0 was added just before it.
		"generic start with a specific end": {
			step:          stepConfig("infinibandConfiguration", addedInterface, lastSrIovInterface),
			events:        multusInterfaceEvents(4),
			expectedStart: new(baselineWithOffset(0).Time),
			expectedEnd:   new(baselineWithOffset(4).Time),
		},
		"generic start, nothing matches the specific end": {
			step:   stepConfig("infinibandConfiguration", addedInterface, lastSrIovInterface),
			events: multusInterfaceEvents(0),
		},
		"start marker never matches": {
			step:   stepConfig("neverHappened", &markerConfig{Reason: "IBConfiguring"}, &markerConfig{Reason: "IBConfigured"}),
			events: []*v1.Event{{Reason: "SomethingElse", Message: "unrelated", FirstTimestamp: baselineWithOffset(10)}},
		},
		"end marker never matches": {
			step:   stepConfig("stalled", &markerConfig{Reason: "IBConfiguring"}, &markerConfig{Reason: "IBConfigured"}),
			events: []*v1.Event{{Reason: "IBConfiguring", Message: "Binding 8 IB devices", FirstTimestamp: baselineWithOffset(10)}},
		},
		// The end is the first match at or after the start, so a reason a plugin reuses across the
		// pod's lifetime cannot produce a step running backwards.
		"stale end before the start is not paired": {
			step: stepConfig("ordered", &markerConfig{Reason: "Start"}, &markerConfig{Reason: "End"}),
			events: []*v1.Event{
				{Reason: "End", Message: "stale end from an earlier attempt", FirstTimestamp: baselineWithOffset(1)},
				{Reason: "Start", Message: "begin", FirstTimestamp: baselineWithOffset(10)},
				{Reason: "End", Message: "real end", FirstTimestamp: baselineWithOffset(14)},
			},
			expectedStart: new(baselineWithOffset(10).Time),
			expectedEnd:   new(baselineWithOffset(14).Time),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			info := timingsWithSteps(t, tc.step).Measure(podWithConditions(), nil, tc.events)

			require.NotNil(t, info)
			if tc.expectedStart == nil {
				assert.Empty(t, info.ExtraSteps)
				return
			}
			require.Len(t, info.ExtraSteps, 1)
			assert.Equal(t, tc.step.Name, info.ExtraSteps[0].Name)
			assert.Equal(t, *tc.expectedStart, mustTimestamp(t, info.ExtraSteps[0].StartedAt))
			assert.Equal(t, *tc.expectedEnd, mustTimestamp(t, info.ExtraSteps[0].FinishedAt))
		})
	}
}

func TestMeasure_ExtraStepsWithAggregatedEvents(t *testing.T) {
	// A plugin retrying with an unchanging message: three occurrences in one object.
	aggregated := func(reason string, first, last metav1.Time) *v1.Event {
		return &v1.Event{
			Reason:         reason,
			Message:        "retrying",
			Count:          3,
			FirstTimestamp: first,
			LastTimestamp:  last,
		}
	}

	tests := map[string]struct {
		step          configuration.StartUpStepConfig
		events        []*v1.Event
		expectedStart *time.Time
		expectedEnd   *time.Time
	}{
		"last match reads the final occurrence": {
			step: stepConfig("retried", &markerConfig{Reason: "Begin"}, &markerConfig{Reason: "Retry", Match: "last"}),
			events: []*v1.Event{
				{Reason: "Begin", Message: "start", FirstTimestamp: baselineWithOffset(1)},
				aggregated("Retry", baselineWithOffset(5), baselineWithOffset(15)),
			},
			expectedStart: new(baselineWithOffset(1).Time),
			expectedEnd:   new(baselineWithOffset(15).Time),
		},
		"last match picks the maximum, not the last scanned": {
			step: stepConfig("overlapping", &markerConfig{Reason: "Begin"}, &markerConfig{Reason: "Retry", Match: "last"}),
			events: []*v1.Event{
				{Reason: "Begin", Message: "start", FirstTimestamp: baselineWithOffset(1)},
				aggregated("Retry", baselineWithOffset(5), baselineWithOffset(20)),
				{Reason: "Retry", Message: "a later first, an earlier last", FirstTimestamp: baselineWithOffset(10)},
			},
			expectedStart: new(baselineWithOffset(1).Time),
			expectedEnd:   new(baselineWithOffset(20).Time),
		},
		"first match with events out of order": {
			step: stepConfig("unordered", &markerConfig{Reason: "Begin"}, &markerConfig{Reason: "End"}),
			events: []*v1.Event{
				{Reason: "Begin", Message: "second attempt", FirstTimestamp: baselineWithOffset(10)},
				nil,
				{Reason: "Begin", Message: "first attempt", FirstTimestamp: baselineWithOffset(2)},
				{Reason: "End", Message: "done", FirstTimestamp: baselineWithOffset(20)},
			},
			expectedStart: new(baselineWithOffset(2).Time),
			expectedEnd:   new(baselineWithOffset(20).Time),
		},
		"last match with events out of order": {
			step: stepConfig("unorderedLast", &markerConfig{Reason: "Begin"}, &markerConfig{Reason: "End", Match: "last"}),
			events: []*v1.Event{
				{Reason: "End", Message: "later end", FirstTimestamp: baselineWithOffset(20)},
				{Reason: "Begin", Message: "start", FirstTimestamp: baselineWithOffset(2)},
				{Reason: "End", Message: "earlier end", FirstTimestamp: baselineWithOffset(12)},
			},
			expectedStart: new(baselineWithOffset(2).Time),
			expectedEnd:   new(baselineWithOffset(20).Time),
		},
		// A first-match marker still wants the original occurrence.
		"first match reads the original occurrence": {
			step: stepConfig("retried", &markerConfig{Reason: "Retry"}, &markerConfig{Reason: "Done"}),
			events: []*v1.Event{
				aggregated("Retry", baselineWithOffset(5), baselineWithOffset(15)),
				{Reason: "Done", Message: "finished", FirstTimestamp: baselineWithOffset(18)},
			},
			expectedStart: new(baselineWithOffset(5).Time),
			expectedEnd:   new(baselineWithOffset(18).Time),
		},
		// An event whose first occurrence precedes the start but which recurred after it is still a
		// valid end: what matters is that an occurrence falls at or after the start.
		"an event that recurred after the start is eligible": {
			step: stepConfig("recurring", &markerConfig{Reason: "Begin"}, &markerConfig{Reason: "Retry", Match: "last"}),
			events: []*v1.Event{
				aggregated("Retry", baselineWithOffset(1), baselineWithOffset(20)),
				{Reason: "Begin", Message: "start", FirstTimestamp: baselineWithOffset(10)},
			},
			expectedStart: new(baselineWithOffset(10).Time),
			expectedEnd:   new(baselineWithOffset(20).Time),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			info := timingsWithSteps(t, tc.step).Measure(podWithConditions(), nil, tc.events)

			require.NotNil(t, info)
			require.Len(t, info.ExtraSteps, 1)
			assert.Equal(t, *tc.expectedStart, mustTimestamp(t, info.ExtraSteps[0].StartedAt))
			assert.Equal(t, *tc.expectedEnd, mustTimestamp(t, info.ExtraSteps[0].FinishedAt))
		})
	}
}
