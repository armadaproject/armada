package eventwatcher

import (
	"regexp"
	"strings"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/pkg/api"
)

func assertEvent(expected *api.EventMessage, actual *api.EventMessage) error {
	switch e := expected.Events.(type) {
	case *api.EventMessage_Pending:
		v := actual.Events.(*api.EventMessage_Pending).Pending
		return assertPodName(e.Pending.GetPodName(), v.GetPodName(), v.GetJobId(), v.GetRunId())
	case *api.EventMessage_Running:
		v := actual.Events.(*api.EventMessage_Running).Running
		return assertPodName(e.Running.GetPodName(), v.GetPodName(), v.GetJobId(), v.GetRunId())
	case *api.EventMessage_Failed:
		v := actual.Events.(*api.EventMessage_Failed)
		if err := assertEventFailed(e, v); err != nil {
			return err
		}
		return assertPodName(e.Failed.GetPodName(), v.Failed.GetPodName(), v.Failed.GetJobId(), v.Failed.GetRunId())
	default:
		return nil
	}
}

// assertPodName compares the pod name of an event with a template. It replaces {JobId} and {RunId} with the IDs of the
// actual event, so a test case can expect either pod name format. An empty template skips the check.
func assertPodName(template string, actual string, jobId string, runId string) error {
	if template == "" {
		return nil
	}
	expected := strings.NewReplacer("{JobId}", jobId, "{RunId}", runId).Replace(template)
	if actual != expected {
		return errors.Errorf("expected pod name %q but got %q", expected, actual)
	}
	return nil
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

	// retryable is compared unconditionally, unlike the category fields above:
	// a testcase expecting a terminal failure (the zero value, retryable=false)
	// must reject a retryable, non-terminal failure rather than pass silently.
	if expected.Failed.GetRetryable() != actual.Failed.GetRetryable() {
		return errors.Errorf("expected retryable=%t but got retryable=%t",
			expected.Failed.GetRetryable(), actual.Failed.GetRetryable())
	}

	return nil
}
