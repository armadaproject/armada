package podchecks

import (
	"fmt"
	"regexp"
	"time"

	config "github.com/G-Research/armada/internal/executor/configuration/podchecks"
	"github.com/G-Research/armada/internal/executor/util"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
)

type containerStateChecks struct {
	checks []containerStatusCheck
}

type containerStatusCheck struct {
	state        config.ContainerState
	reasonRegexp *regexp.Regexp
	timeout      time.Duration
	action       config.Action
}

func newContainerStateChecks(configs []config.ContainerStatusCheck) (*containerStateChecks, error) {
	containerStateChecks := &containerStateChecks{}
	for _, config := range configs {
		re, err := regexp.Compile(config.ReasonRegexp)
		if err != nil {
			return nil, fmt.Errorf("Cannot parse regexp \"%s\": %v", config.ReasonRegexp, err)
		}
		containerStateChecks.checks = append(containerStateChecks.checks, containerStatusCheck{reasonRegexp: re, action: config.Action, timeout: config.Timeout, state: config.State})
	}
	return containerStateChecks, nil
}

func (csc *containerStateChecks) getAction(pod *v1.Pod, timeInState time.Duration) (Action, string) {
	for _, check := range csc.checks {
		for _, containerStatus := range util.GetPodContainerStatuses(pod) {
			if containerStatus.State.Waiting != nil {
				reason := containerStatus.State.Waiting.Reason
				message := containerStatus.State.Waiting.Message
				state := config.ContainerStateWaiting
				if check.reasonRegexp.MatchString(reason) && state == check.state && timeInState >= check.timeout {
					log.Warnf("Returning action %s for pod %s in namespace %s because container %s has been in state %s with reason %s (%s) for more than %v", check.action, pod.Name, pod.Namespace, containerStatus.Name, state, reason, message, check.timeout)
					return mapAction(check.action), fmt.Sprintf("Container %s has been in state %s for reason %s (%s) for more than timeout %v", containerStatus.Name, state, reason, message, check.timeout)
				}
			}
		}
	}
	return ActionWait, ""
}
