package podchecks

import (
	"fmt"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	config "github.com/G-Research/armada/internal/executor/configuration/podchecks"
)

type eventChecker interface {
	getAction(podName string, podEvents []*v1.Event) (Action, string)
}

type eventCheck struct {
	regexp    *regexp.Regexp
	inverse   bool
	action    Action
	eventType config.EventType
}

type eventChecks struct {
	checks []eventCheck
}

func newEventChecks(configs []config.EventCheck) (*eventChecks, error) {
	eventChecks := &eventChecks{}
	for _, config := range configs {
		re, err := regexp.Compile(config.Regexp)
		if err != nil {
			return nil, fmt.Errorf("Cannot parse regexp \"%s\": %+v", config.Regexp, err)
		}

		action, err := mapAction(config.Action)
		if err != nil {
			return nil, err
		}

		if string(config.Type) != v1.EventTypeNormal && string(config.Type) != v1.EventTypeWarning {
			return nil, fmt.Errorf("Invalid event type: \"%s\"", config.Type)
		}

		check := eventCheck{regexp: re, inverse: config.Inverse, action: action, eventType: config.Type}
		eventChecks.checks = append(eventChecks.checks, check)
		log.Infof("   Created event check %s %s\"%s\" %s", check.eventType, inverseString(check.inverse), check.regexp.String(), check.action)
	}
	return eventChecks, nil
}

func (ec *eventChecks) getAction(podName string, podEvents []*v1.Event) (Action, string) {
	resultAction := ActionWait
	resultMessages := []string{}
	for _, check := range ec.checks {
		for _, event := range podEvents {
			if event.Type == string(check.eventType) && (check.inverse != check.regexp.MatchString(event.Message)) {
				log.Warnf("Pod %s needs action %s %s because event \"%s\" matches regexp %s\"%v\"", podName, check.action, check.action, event.Message, inverseString(check.inverse), check.regexp)
				resultAction = maxAction(resultAction, check.action)
				resultMessages = append(resultMessages, event.Message)
			}
		}
	}

	return resultAction, strings.Join(resultMessages, "\n")
}

func inverseString(inverse bool) string {
	if inverse {
		return "NOT "
	} else {
		return ""
	}
}
