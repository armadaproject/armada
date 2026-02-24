package retry

import (
	"slices"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Matcher evaluates failure info against policy rules.
type Matcher struct{}

// NewMatcher creates a new Matcher.
func NewMatcher() *Matcher {
	return &Matcher{}
}

// Match evaluates the failure info against the policy rules and returns the action.
// Rules are evaluated in order; the first matching rule wins.
// If no rule matches, ActionFail is returned (implicit fail).
func (m *Matcher) Match(failureInfo *armadaevents.FailureInfo, rules []configuration.Rule) configuration.Action {
	if failureInfo == nil {
		return configuration.ActionFail
	}

	for _, rule := range rules {
		if matchesRule(failureInfo, rule) {
			return rule.Action
		}
	}

	return configuration.ActionFail
}

// matchesRule checks one matcher per rule (first non-nil wins: conditions, then exit codes, then message).
func matchesRule(info *armadaevents.FailureInfo, rule configuration.Rule) bool {
	if len(rule.OnConditions) > 0 {
		return matchesCondition(info.Condition, rule.OnConditions)
	}
	if rule.OnExitCodes != nil {
		return matchesExitCode(info.ExitCode, rule.OnExitCodes)
	}
	if rule.OnTerminationMessage != nil {
		return matchesTerminationMessage(info.TerminationMessage, rule.OnTerminationMessage)
	}
	return false
}

func matchesCondition(cond armadaevents.FailureCondition, ruleConditions []configuration.FailureCondition) bool {
	return slices.Contains(ruleConditions, mapProtoCondition(cond))
}

// Zero is ambiguous (proto3 default vs success), negative doesn't occur in K8s.
func matchesExitCode(exitCode int32, matcher *configuration.ExitCodeMatcher) bool {
	if exitCode <= 0 {
		return false
	}
	return matcher.Matches(exitCode)
}

func matchesTerminationMessage(message string, matcher *configuration.RegexMatcher) bool {
	return matcher.Matches(message)
}

// mapProtoCondition returns "" for unknown conditions, which won't match any rule.
func mapProtoCondition(cond armadaevents.FailureCondition) configuration.FailureCondition {
	switch cond {
	case armadaevents.FailureCondition_FAILURE_CONDITION_PREEMPTED:
		return configuration.ConditionPreempted
	case armadaevents.FailureCondition_FAILURE_CONDITION_EVICTED:
		return configuration.ConditionEvicted
	case armadaevents.FailureCondition_FAILURE_CONDITION_OOM_KILLED:
		return configuration.ConditionOOMKilled
	case armadaevents.FailureCondition_FAILURE_CONDITION_DEADLINE_EXCEEDED:
		return configuration.ConditionDeadlineExceeded
	case armadaevents.FailureCondition_FAILURE_CONDITION_UNSCHEDULABLE:
		return configuration.ConditionUnschedulable
	default:
		return ""
	}
}
