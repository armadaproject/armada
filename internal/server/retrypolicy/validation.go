package retrypolicy

import (
	"fmt"
	"regexp"

	"github.com/armadaproject/armada/pkg/api"
)

// Policy names follow RFC 1123 label rules because they may end up in
// Kubernetes labels.
var policyNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

const maxPolicyNameLength = 63

// ValidatePolicy checks that a retry policy is structurally valid, so that
// malformed policies are rejected at write time.
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
	if !isValidAction(p.DefaultAction) {
		return fmt.Errorf("retry policy %q must set a default action (Fail or Retry)", p.Name)
	}
	return nil
}

func validateRule(r *api.RetryRule) error {
	if r == nil {
		return fmt.Errorf("rule must not be nil")
	}
	if !isValidAction(r.Action) {
		return fmt.Errorf("action must be Fail or Retry")
	}
	// on_subcategory only narrows an on_category match, so it does not count
	// as a matcher on its own.
	if r.OnCategory == "" {
		return fmt.Errorf("on_category must be set")
	}
	return nil
}

// isValidAction rejects RETRY_ACTION_UNSPECIFIED, so every action is explicit.
func isValidAction(action api.RetryAction) bool {
	return action == api.RetryAction_RETRY_ACTION_FAIL || action == api.RetryAction_RETRY_ACTION_RETRY
}
