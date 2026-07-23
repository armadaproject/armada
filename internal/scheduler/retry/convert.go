package retry

import (
	"fmt"

	"github.com/armadaproject/armada/pkg/api"
)

// ConvertPolicy translates an api.RetryPolicy proto into the internal Policy,
// validating fields. Returns an error for any malformed field (unknown action,
// missing category, etc.).
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
	if err := ValidatePolicy(*policy); err != nil {
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
		// error. Silently defaulting could turn a truncated proto into a
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

	return Rule{
		Action:        action,
		OnCategory:    r.OnCategory,
		OnSubcategory: r.OnSubcategory,
		Mutation: Mutation{
			NodeAntiAffinity: r.GetMutate().GetNodeAntiAffinity(),
		},
	}, nil
}
