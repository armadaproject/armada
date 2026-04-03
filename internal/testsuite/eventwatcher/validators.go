package eventwatcher

import (
	"regexp"
	"slices"
	"strings"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/pkg/api"
)

func assertEvent(expected *api.EventMessage, actual *api.EventMessage) error {
	switch e := expected.Events.(type) {
	case *api.EventMessage_Failed:
		v := actual.Events.(*api.EventMessage_Failed)
		return assertEventFailed(e, v)
	default:
		return nil
	}
}

func assertEventFailed(expected *api.EventMessage_Failed, actual *api.EventMessage_Failed) error {
	if actual == nil {
		return errors.Errorf("unexpected nil event 'actual'")
	}

	if reason := expected.Failed.GetReason(); reason != "" {
		re, err := regexp.Compile(reason)
		if err != nil {
			return errors.Errorf("failed to compile regex %q: %v", reason, err)
		}
		if !re.MatchString(actual.Failed.GetReason()) {
			return errors.Errorf(
				"error asserting failure reason: expected %s, got %s",
				reason, actual.Failed.GetReason(),
			)
		}
	}

	if len(expected.Failed.GetCategories()) > 0 {
		if err := assertCategories(expected.Failed.GetCategories(), actual.Failed.GetCategories()); err != nil {
			return err
		}
	}

	return nil
}

func assertCategories(expected, actual []string) error {
	exp := slices.Clone(expected)
	slices.Sort(exp)

	act := slices.Clone(actual)
	slices.Sort(act)

	if !slices.Equal(exp, act) {
		return errors.Errorf("expected categories [%s] but got [%s]",
			strings.Join(exp, ", "), strings.Join(act, ", "))
	}
	return nil
}
