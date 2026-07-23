package retrypolicy

import (
	"fmt"
	"regexp"

	"github.com/armadaproject/armada/pkg/api"
)

// Policy names follow RFC 1123 label rules, like queue names. Names are used
// as stable references from queues and may end up in Kubernetes labels, so
// anything looser risks breaking downstream consumers.
var policyNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

const maxPolicyNameLength = 63

// ValidatePolicy performs structural validation of a retry policy at write
// time. The scheduler-side cache loader silently drops policies it cannot
// compile, so rejecting malformed policies here is the only way the operator
// gets immediate feedback instead of a policy that never takes effect.
func ValidatePolicy(p *api.RetryPolicy) error {
	if p == nil {
		return fmt.Errorf("retry policy must not be nil")
	}
	if p.Name == "" {
		return fmt.Errorf("retry policy name must not be empty")
	}
	if len(p.Name) > maxPolicyNameLength {
		return fmt.Errorf("retry policy name %q must be at most %d characters", p.Name, maxPolicyNameLength)
	}
	if !policyNamePattern.MatchString(p.Name) {
		return fmt.Errorf(
			"retry policy name %q is invalid: must consist of lowercase alphanumeric characters or '-', and must start and end with an alphanumeric character",
			p.Name,
		)
	}
	for i, rule := range p.Rules {
		if err := validateRule(rule); err != nil {
			return fmt.Errorf("retry policy %q rule %d: %w", p.Name, i, err)
		}
	}
	// The scheduler's policy loader accepts only Fail or Retry, so reject
	// anything else here too rather than accept a policy the scheduler will
	// silently drop.
	if p.DefaultAction != api.RetryAction_RETRY_ACTION_FAIL && p.DefaultAction != api.RetryAction_RETRY_ACTION_RETRY {
		return fmt.Errorf("retry policy %q must set a default action (Fail or Retry)", p.Name)
	}
	return nil
}

func validateRule(r *api.RetryRule) error {
	if r == nil {
		return fmt.Errorf("rule must not be nil")
	}
	if r.Action != api.RetryAction_RETRY_ACTION_FAIL && r.Action != api.RetryAction_RETRY_ACTION_RETRY {
		return fmt.Errorf("action must be Fail or Retry")
	}
	// Rules match on category only. on_subcategory only narrows an on_category
	// match, so it does not count as a matcher on its own.
	if r.OnCategory == "" {
		return fmt.Errorf("on_category must be set")
	}
	return nil
}
