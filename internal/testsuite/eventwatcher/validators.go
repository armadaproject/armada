package eventwatcher

import (
	"regexp"

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

	if cat := expected.Failed.GetFailureCategory(); cat != "" && cat != actual.Failed.GetFailureCategory() {
		return errors.Errorf("expected failure_category %q but got %q", cat, actual.Failed.GetFailureCategory())
	}

	if sub := expected.Failed.GetFailureSubcategory(); sub != "" && sub != actual.Failed.GetFailureSubcategory() {
		return errors.Errorf("expected failure_subcategory %q but got %q", sub, actual.Failed.GetFailureSubcategory())
	}

	if expected.Failed.GetRetryable() != actual.Failed.GetRetryable() {
		return errors.Errorf("expected retryable=%t but got retryable=%t",
			expected.Failed.GetRetryable(), actual.Failed.GetRetryable())
	}

	return nil
}
