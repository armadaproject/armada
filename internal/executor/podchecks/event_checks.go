package podchecks

import (
	"fmt"
	"regexp"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	config "github.com/G-Research/armada/internal/executor/configuration/podchecks"
)

type eventCheck struct {
	regexp *regexp.Regexp
	action config.Action
}

type eventChecks struct {
	checks []eventCheck
}

func newEventChecks(configs []config.EventCheck) (*eventChecks, error) {
	eventChecks := &eventChecks{}
	for _, config := range configs {
		re, err := regexp.Compile(config.Regexp)
		if err != nil {
			return nil, fmt.Errorf("Cannot parse regexp \"%s\": %v", config.Regexp, err)
		}
		eventChecks.checks = append(eventChecks.checks, eventCheck{regexp: re, action: config.Action})
	}
	return eventChecks, nil
}

func (ec *eventChecks) getAction(podName string, podEvents []*v1.Event) (Action, string) {
	for _, check := range ec.checks {
		for _, event := range podEvents {
			if check.regexp.MatchString(event.Message) {
				log.Warnf("Returning action %s for pod %s because event \"%s\" matches regexp \"%v\"", check.action, podName, event.Message, check.regexp)
				return mapAction(check.action), event.Message
			}
		}
	}
	return ActionWait, ""
}
