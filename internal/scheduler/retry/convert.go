package retry

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common/errormatch"
	"github.com/armadaproject/armada/pkg/api"
)

// ConvertPolicy translates an api.RetryPolicy proto into the internal Policy,
// validating fields and pre-compiling regex patterns via CompileRules.
// Returns an error for any malformed field (invalid regex, unknown action,
// missing values, etc.).
func ConvertPolicy(p *api.RetryPolicy) (*Policy, error) {
	if p == nil {
		return nil, fmt.Errorf("retry policy is nil")
	}

	defaultAction, err := convertAction(p.DefaultAction)
	if err != nil {
		return nil, fmt.Errorf("default_action: %w", err)
	}

	rules := make([]Rule, 0, len(p.Rules))
	for i, r := range p.Rules {
		converted, err := convertRule(r)
		if err != nil {
			return nil, fmt.Errorf("rule %d: %w", i, err)
		}
		rules = append(rules, converted)
	}

	policy := &Policy{
		Name:          p.Name,
		RetryLimit:    p.RetryLimit,
		DefaultAction: defaultAction,
		Rules:         rules,
	}
	if err := CompileRules(policy.Rules); err != nil {
		return nil, fmt.Errorf("policy %q: %w", p.Name, err)
	}
	return policy, nil
}

func convertAction(a api.RetryAction) (Action, error) {
	switch a {
	case api.RetryAction_RETRY_ACTION_FAIL:
		return ActionFail, nil
	case api.RetryAction_RETRY_ACTION_RETRY:
		return ActionRetry, nil
	default:
		// Treat RETRY_ACTION_UNSPECIFIED and any unknown value as a hard
		// error; silently defaulting could turn a truncated proto into a
		// policy that retries everything.
		return "", fmt.Errorf("unknown action %q", a.String())
	}
}

func convertRule(r *api.RetryRule) (Rule, error) {
	if r == nil {
		return Rule{}, fmt.Errorf("rule is nil")
	}
	action, err := convertAction(r.Action)
	if err != nil {
		return Rule{}, fmt.Errorf("action: %w", err)
	}

	// Deep-copy slices so the cached Policy does not retain references into
	// the API response (which becomes GC-eligible after this call returns).
	rule := Rule{
		Action:        action,
		OnConditions:  append([]string(nil), r.OnConditions...),
		OnCategory:    r.OnCategory,
		OnSubcategory: r.OnSubcategory,
	}
	if r.OnExitCodes != nil {
		op, err := convertExitCodeOperator(r.OnExitCodes.Operator)
		if err != nil {
			return Rule{}, fmt.Errorf("on_exit_codes: %w", err)
		}
		rule.OnExitCodes = &errormatch.ExitCodeMatcher{
			Operator: op,
			Values:   append([]int32(nil), r.OnExitCodes.Values...),
		}
	}
	if r.OnTerminationMessagePattern != "" {
		rule.OnTerminationMessage = &errormatch.RegexMatcher{Pattern: r.OnTerminationMessagePattern}
	}
	return rule, nil
}

func convertExitCodeOperator(op api.ExitCodeOperator) (errormatch.ExitCodeOperator, error) {
	switch op {
	case api.ExitCodeOperator_EXIT_CODE_OPERATOR_IN:
		return errormatch.ExitCodeOperatorIn, nil
	case api.ExitCodeOperator_EXIT_CODE_OPERATOR_NOT_IN:
		return errormatch.ExitCodeOperatorNotIn, nil
	default:
		return "", fmt.Errorf("unknown operator %q", op.String())
	}
}
