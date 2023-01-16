package podchecks

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	config "github.com/armadaproject/armada/internal/executor/configuration/podchecks"
)

type eventChecker interface {
	getAction(podName string, podEvents []*v1.Event, timeInState time.Duration) (Action, string)
}

type eventCheck struct {
	regexp      *regexp.Regexp
	inverse     bool
	eventType   config.EventType
	gracePeriod time.Duration
	action      Action
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

		check := eventCheck{regexp: re, inverse: config.Inverse, eventType: config.Type, gracePeriod: config.GracePeriod, action: action}
		eventChecks.checks = append(eventChecks.checks, check)
		log.Infof(
			"   Created event check %s %s\"%s\" %s %s",
			check.eventType,
			inverseString(check.inverse),
			check.regexp.String(),
			check.gracePeriod,
			check.action,
		)
	}
	return eventChecks, nil
}

func (ec *eventChecks) getAction(podName string, podEvents []*v1.Event, timeInState time.Duration) (Action, string) {
	resultAction := ActionWait
	resultMessages := []string{}
	for _, event := range podEvents {
		action, message := ec.getEventAction(podName, event, timeInState)
		resultAction = maxAction(resultAction, action)
		resultMessages = append(resultMessages, message)
	}
	return resultAction, strings.Join(resultMessages, "\n")
}

func (ec *eventChecks) getEventAction(podName string, podEvent *v1.Event, timeInState time.Duration) (Action, string) {
	for _, check := range ec.checks {
		if podEvent.Type == string(check.eventType) && (check.inverse != check.regexp.MatchString(podEvent.Message)) {
			if timeInState > check.gracePeriod {
				log.Warnf(
					"Pod %s needs action %s because event \"%s\" matches regexp %s\"%v\"",
					podName,
					check.action,
					podEvent.Message,
					inverseString(check.inverse),
					check.regexp,
				)
				return check.action, podEvent.Message
			} else {
				return ActionWait, ""
			}
		}
	}
	return ActionWait, ""
}
