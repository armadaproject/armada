package retry

import (
	"fmt"

	"github.com/armadaproject/armada/pkg/api"
)

// ConvertPolicy translates an api.RetryPolicy proto into the internal Policy.
// It only parses. The CRUD service validates policies at write time, so
// conversion assumes the stored policy is valid. It still returns an error
// for data it cannot map, for example an unknown action enum value.
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
	}, nil
}
