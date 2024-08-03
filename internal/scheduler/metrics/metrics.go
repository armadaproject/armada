package metrics

import (
	"regexp"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
)

type Metrics struct {
	*cycleMetrics
	*jobStateMetrics
}

func New(errorRegexes []string, trackedResourceNames []v1.ResourceName) (*Metrics, error) {
	compiledErrorRegexes := make([]*regexp.Regexp, len(errorRegexes))
	for i, errorRegex := range errorRegexes {
		if r, err := regexp.Compile(errorRegex); err != nil {
			return nil, errors.WithStack(err)
		} else {
			compiledErrorRegexes[i] = r
		}
	}
	return &Metrics{
		cycleMetrics:    newCycleMetrics(),
		jobStateMetrics: newJobStateMetrics(compiledErrorRegexes, trackedResourceNames),
	}, nil
}
