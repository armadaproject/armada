package failedpodchecks

import (
	"fmt"
	"regexp"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/executor/configuration/podchecks"
)

type podStatusRetryChecker interface {
	IsRetryable(pod *v1.Pod) (bool, string)
}

type podStatusCheck struct {
	regexp *regexp.Regexp
	reason string
}

type PodStatusChecker struct {
	checks []podStatusCheck
}

func NewPodStatusChecker(checks []podchecks.PodStatusCheck) (*PodStatusChecker, error) {
	podStatusChecks := make([]podStatusCheck, 0, len(checks))

	for _, check := range checks {
		re, err := regexp.Compile(check.Regexp)
		if err != nil {
			return nil, fmt.Errorf("cannot parse regexp \"%s\": %+v", check.Regexp, err)
		}

		podStatusChecks = append(podStatusChecks, podStatusCheck{
			regexp: re,
			reason: check.Reason,
		})
	}
	return &PodStatusChecker{
		checks: podStatusChecks,
	}, nil
}

func (f *PodStatusChecker) IsRetryable(pod *v1.Pod) (bool, string) {
	for _, check := range f.checks {
		if check.reason != "" && check.reason != pod.Status.Reason {
			continue
		}

		if check.regexp.MatchString(pod.Status.Message) {
			return true, "message"
		}
	}

	return false, ""
}
