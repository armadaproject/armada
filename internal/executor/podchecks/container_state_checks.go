package podchecks

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	config "github.com/armadaproject/armada/internal/executor/configuration/podchecks"
	"github.com/armadaproject/armada/internal/executor/util"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
)

type containerStateChecker interface {
	getAction(pod *v1.Pod, timeInState time.Duration) (Action, string)
}

type containerStateChecks struct {
	checks []containerStatusCheck
}

type containerStatusCheck struct {
	state        config.ContainerState
	reasonRegexp *regexp.Regexp
	inverse      bool
	gracePeriod  time.Duration
	action       Action
}

func newContainerStateChecks(configs []config.ContainerStatusCheck) (*containerStateChecks, error) {
	containerStateChecks := &containerStateChecks{}
	for _, cfg := range configs {

		re, err := regexp.Compile(cfg.ReasonRegexp)
		if err != nil {
			return nil, fmt.Errorf("Cannot parse regexp \"%s\": %+v", cfg.ReasonRegexp, err)
		}

		action, err := mapAction(cfg.Action)
		if err != nil {
			return nil, err
		}

		if cfg.State != config.ContainerStateWaiting {
			return nil, fmt.Errorf("Invalid container state: \"%s\"", cfg.State)
		}

		check := containerStatusCheck{reasonRegexp: re, inverse: cfg.Inverse, action: action, gracePeriod: cfg.GracePeriod, state: cfg.State}
		containerStateChecks.checks = append(containerStateChecks.checks, check)
		log.Infof(
			"   Created container state check %s %s\"%s\" %s %s",
			check.state,
			inverseString(check.inverse),
			check.reasonRegexp,
			check.gracePeriod,
			check.action,
		)
	}
	return containerStateChecks, nil
}

func (csc *containerStateChecks) getAction(pod *v1.Pod, timeInState time.Duration) (Action, string) {
	resultAction := ActionWait
	resultMessages := []string{}

	for _, containerStatus := range util.GetPodContainerStatuses(pod) {
		action, message := csc.getContainerAction(pod, &containerStatus, timeInState)

		resultAction = maxAction(resultAction, action)
		resultMessages = append(resultMessages, message)

	}
	return resultAction, strings.Join(resultMessages, "\n")
}

func (csc *containerStateChecks) getContainerAction(pod *v1.Pod, containerStatus *v1.ContainerStatus, timeInState time.Duration) (Action, string) {
	for _, check := range csc.checks {
		// For now we only support checks on containers in the Waiting state
		if containerStatus.State.Waiting != nil {
			reason := containerStatus.State.Waiting.Reason
			message := containerStatus.State.Waiting.Message
			state := config.ContainerStateWaiting

			if check.inverse != check.reasonRegexp.MatchString(reason) && state == check.state {
				if timeInState >= check.gracePeriod {
					log.Warnf(
						"Container %s in Pod %s in namespace %s has been in state %s with reason %s (%s) for more than %v (matched regexp was %s%s), required action is %s",
						containerStatus.Name,
						pod.Name,
						pod.Namespace,
						state,
						reason,
						message,
						check.gracePeriod,
						inverseString(check.inverse),
						check.reasonRegexp,
						check.action,
					)
					return check.action, fmt.Sprintf(
						"Container %s has been in state %s for reason %s (%s) for more than timeout %v",
						containerStatus.Name,
						state,
						reason,
						message,
						check.gracePeriod,
					)
				} else {
					return ActionWait, ""
				}
			}
		}
	}
	return ActionWait, ""
}
