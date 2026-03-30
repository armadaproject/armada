package retry

import (
	"slices"

	"github.com/armadaproject/armada/internal/common/errormatch"
)

// matchInput bundles every signal a rule may match against, so adding a new
// matcher does not ripple through every signature in the package.
type matchInput struct {
	condition          string
	exitCode           int32
	terminationMessage string
	category           string
	subcategory        string
}

// matchRule returns true if all non-empty match fields on the rule match the given input.
// Empty/nil match fields are ignored (they match anything).
func matchRule(rule *Rule, in matchInput) bool {
	if len(rule.OnConditions) > 0 {
		if !slices.Contains(rule.OnConditions, in.condition) {
			return false
		}
	}
	if rule.OnExitCodes != nil {
		if !errormatch.MatchExitCode(rule.OnExitCodes, in.exitCode) {
			return false
		}
	}
	if rule.OnTerminationMessage != nil {
		// Fail closed if CompileRules was not called: an uncompiled pattern
		// must never vacuously match every error.
		if rule.compiledTerminationMessage == nil {
			return false
		}
		if !errormatch.MatchPattern(rule.compiledTerminationMessage, in.terminationMessage) {
			return false
		}
	}
	if rule.OnCategory != "" {
		if in.category != rule.OnCategory {
			return false
		}
		if rule.OnSubcategory != "" && in.subcategory != rule.OnSubcategory {
			return false
		}
	}
	return true
}

// matchRules iterates rules in order and returns a pointer to the first
// matching rule. Returns nil if no rule matches.
func matchRules(rules []Rule, in matchInput) *Rule {
	for i := range rules {
		if matchRule(&rules[i], in) {
			return &rules[i]
		}
	}
	return nil
}
