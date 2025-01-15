package failedpodchecks

import (
	"fmt"
	"regexp"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/executor/configuration/podchecks"
)

type eventRetryChecker interface {
	IsRetryable(podEvents []*v1.Event) (bool, string)
}

type podEventCheck struct {
	regexp    *regexp.Regexp
	reason    string
	eventType podchecks.EventType
}

type PodEventsChecker struct {
	checks []podEventCheck
}

func NewPodEventsChecker(checks []podchecks.PodEventCheck) (*PodEventsChecker, error) {
	podEventChecks := make([]podEventCheck, 0, len(checks))

	for _, check := range checks {
		re, err := regexp.Compile(check.Regexp)
		if err != nil {
			return nil, fmt.Errorf("cannot parse regexp \"%s\": %+v", check.Regexp, err)
		}
		if string(check.Type) != v1.EventTypeNormal && string(check.Type) != v1.EventTypeWarning {
			return nil, fmt.Errorf("invalid event type: \"%s\"", check.Type)
		}

		podEventChecks = append(podEventChecks, podEventCheck{
			regexp:    re,
			eventType: check.Type,
			reason:    check.Reason,
		})
	}

	return &PodEventsChecker{
		checks: podEventChecks,
	}, nil
}

func (f *PodEventsChecker) IsRetryable(podEvents []*v1.Event) (bool, string) {
	for _, check := range f.checks {
		for _, podEvent := range podEvents {
			if check.reason != "" && check.reason != podEvent.Reason {
				continue
			}

			if string(check.eventType) != podEvent.Type {
				continue
			}

			if check.regexp.MatchString(podEvent.Message) {
				return true, "message"
			}
		}
	}

	return false, ""
}
