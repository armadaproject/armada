package failedpodchecks

import (
	"fmt"
	"regexp"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/executor/configuration/podchecks"
)

type failedContainerStatusRetryChecker interface {
	IsRetryable(pod *v1.Pod) (bool, string)
}

type failedContainerStatusCheck struct {
	containerNameRegexp *regexp.Regexp
	messageRegexp       *regexp.Regexp
}

type FailedContainerStatusChecker struct {
	checks []failedContainerStatusCheck
}

func NewFailedContainerStatusChecker(checks []podchecks.FailedContainerStatusCheck) (*FailedContainerStatusChecker, error) {
	podContainerStatusChecks := make([]failedContainerStatusCheck, 0, len(checks))

	for _, check := range checks {
		containerNameRegex, err := regexp.Compile(check.ContainerNameRegexp)
		if err != nil {
			return nil, fmt.Errorf("cannot parse regexp \"%s\": %+v", check.ContainerNameRegexp, err)
		}
		messageRegex, err := regexp.Compile(check.MessageRegexp)
		if err != nil {
			return nil, fmt.Errorf("cannot parse regexp \"%s\": %+v", check.MessageRegexp, err)
		}

		podContainerStatusChecks = append(podContainerStatusChecks, failedContainerStatusCheck{
			containerNameRegexp: containerNameRegex,
			messageRegexp:       messageRegex,
		})
	}
	return &FailedContainerStatusChecker{
		checks: podContainerStatusChecks,
	}, nil
}

func (f *FailedContainerStatusChecker) IsRetryable(pod *v1.Pod) (bool, string) {
	containers := pod.Status.ContainerStatuses
	containers = append(containers, pod.Status.InitContainerStatuses...)

	for _, check := range f.checks {
		for _, container := range containers {
			terminatedContainer := container.State.Terminated
			if terminatedContainer != nil {
				if check.containerNameRegexp.MatchString(container.Name) {
					if check.messageRegexp.MatchString(terminatedContainer.Message) {
						return true, terminatedContainer.Message
					}
				}
			}

		}
	}

	return false, ""
}
