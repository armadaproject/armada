package startup

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"

	v1 "k8s.io/api/core/v1"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// compiledStep is a configured extra step with its message patterns already compiled, so a bad
// pattern fails the executor at startup rather than quietly never matching at runtime.
type compiledStep struct {
	name  string
	start eventMarker
	end   eventMarker
}

// Match modes for a marker, naming which of several matching events to take.
const (
	matchFirst = "first"
	matchLast  = "last"
)

type eventMarker struct {
	reason  *regexp.Regexp
	message *regexp.Regexp
	// takeLast selects the latest matching event rather than the earliest.
	takeLast bool
}

// matches is true when every pattern the operator set is satisfied. An unset pattern is not
// checked, but a marker with none at all is rejected at construction.
func (m eventMarker) matches(event *v1.Event) bool {
	if m.reason != nil && !m.reason.MatchString(event.Reason) {
		return false
	}
	if m.message != nil && !m.message.MatchString(event.Message) {
		return false
	}
	return true
}

func compileSteps(configured []configuration.StartUpStepConfig) ([]compiledStep, error) {
	steps := make([]compiledStep, 0, len(configured))
	seen := make(map[string]bool, len(configured))
	for _, step := range configured {
		if step.Name == "" {
			return nil, errors.New("startUpTimings extraStep must have a name")
		}
		// Names identify a step in the event and will become a metric label, so a duplicate would
		// silently produce two series under one name.
		if seen[step.Name] {
			return nil, fmt.Errorf("startUpTimings extraStep %q is defined more than once", step.Name)
		}
		seen[step.Name] = true

		start, err := compileEventMarker(step.Name, "start", step.Start)
		if err != nil {
			return nil, err
		}
		end, err := compileEventMarker(step.Name, "end", step.End)
		if err != nil {
			return nil, err
		}
		steps = append(steps, compiledStep{name: step.Name, start: start, end: end})
	}
	return steps, nil
}

func compileEventMarker(stepName, field string, matcher *configuration.StartUpMarkerConfig) (eventMarker, error) {
	if matcher == nil {
		return eventMarker{}, fmt.Errorf("startUpTimings extraStep %q must specify %s", stepName, field)
	}
	// Without either pattern the marker would match the first event of any kind, yielding a
	// duration that looks plausible and means nothing.
	if matcher.Reason == "" && matcher.Message == "" {
		return eventMarker{}, fmt.Errorf(
			"startUpTimings extraStep %q %s must specify a reason, a message, or both", stepName, field)
	}

	switch matcher.Match {
	case "", matchFirst, matchLast:
	default:
		return eventMarker{}, fmt.Errorf(
			"startUpTimings extraStep %q %s has an unknown match %q, must be %q or %q",
			stepName, field, matcher.Match, matchFirst, matchLast)
	}

	marker := eventMarker{takeLast: matcher.Match == matchLast}
	for _, pattern := range []struct {
		field   string
		source  string
		compile **regexp.Regexp
	}{
		{"reason", matcher.Reason, &marker.reason},
		{"message", matcher.Message, &marker.message},
	} {
		if pattern.source == "" {
			continue
		}
		compiled, err := regexp.Compile(pattern.source)
		if err != nil {
			return eventMarker{}, fmt.Errorf(
				"startUpTimings extraStep %q %s has an invalid %s pattern %q: %w",
				stepName, field, pattern.field, pattern.source, err)
		}
		*pattern.compile = compiled
	}
	return marker, nil
}

// measureExtraSteps pairs each step's markers against the pod's events, reporting both instants so
// a consumer can place the step on the pod's timeline and tell whether it overlaps a built-in
// interval.
//
// Both markers must match or the step is omitted.
func measureExtraSteps(steps []compiledStep, podEvents []*v1.Event) []*armadaevents.StartUpStep {
	if len(steps) == 0 {
		return nil
	}
	var measured []*armadaevents.StartUpStep
	for _, step := range steps {
		start := matchAtOrAfter(podEvents, step.start, nil)
		if start == nil {
			continue
		}
		// Constrained to at or after the start to prevent stale earlier events being taken as the latest.
		end := matchAtOrAfter(podEvents, step.end, start)
		if end == nil {
			continue
		}
		measured = append(measured, &armadaevents.StartUpStep{
			Name:       step.name,
			StartedAt:  toTimestamp(start),
			FinishedAt: toTimestamp(end),
		})
	}
	return measured
}

// matchAtOrAfter returns the occurrence of a matching event at or after notBefore, which may be nil
// to mean no lower bound.
func matchAtOrAfter(podEvents []*v1.Event, marker eventMarker, notBefore *time.Time) *time.Time {
	// A last-match marker cares when an event was last seen, every other marker when it was first.
	getOccurrenceTime := eventTime
	if marker.takeLast {
		getOccurrenceTime = lastEventOccurrence
	}

	for _, event := range orderedByOccurrence(podEvents, getOccurrenceTime, marker.takeLast) {
		occurrenceTime := getOccurrenceTime(event)
		if notBefore != nil && occurrenceTime.Before(*notBefore) {
			continue
		}
		if marker.matches(event) {
			return new(occurrenceTime)
		}
	}
	return nil
}

// orderedByOccurrence sorts the pod's events by the occurrence a marker selects on, descending for
// a last-match marker so that taking the first match yields the latest.
func orderedByOccurrence(podEvents []*v1.Event, getOccurrenceTime func(*v1.Event) time.Time, descending bool) []*v1.Event {
	ordered := armadaslices.Filter(podEvents, func(event *v1.Event) bool { return event != nil })
	slices.SortStableFunc(ordered, func(a, b *v1.Event) int {
		if descending {
			return getOccurrenceTime(b).Compare(getOccurrenceTime(a))
		}
		return getOccurrenceTime(a).Compare(getOccurrenceTime(b))
	})
	return ordered
}
