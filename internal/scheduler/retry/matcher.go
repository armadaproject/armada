package retry

import (
	"slices"

	"github.com/armadaproject/armada/internal/common/errormatch"
)

// matchInput bundles every signal a rule may match against.
//
// isContainerFailure gates OnTerminationMessage matching: lease returns and
// preemptions have no termination message, so a permissive pattern like ".*"
// would otherwise match their empty default and silently fire.
type matchInput struct {
	condition          string
	exitCode           int32
	terminationMessage string
	isContainerFailure bool
	category           string
	subcategory        string
}

// matchRule returns true if all non-empty match fields on the rule match the
// given input. Empty/nil match fields are ignored (they match anything).
func matchRule(rule *Rule, in matchInput) bool {
	if len(rule.OnConditions) > 0 && !slices.Contains(rule.OnConditions, in.condition) {
		return false
	}
	if rule.OnExitCodes != nil && !errormatch.MatchExitCode(rule.OnExitCodes, in.exitCode) {
		return false
	}
	if rule.OnTerminationMessage != nil {
		// Fail closed if CompileRules was not called: an uncompiled pattern
		// must never vacuously match every error.
		if !in.isContainerFailure || rule.compiledTerminationMessage == nil {
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

// matchRules returns the first rule that matches in, or nil if none do.
func matchRules(rules []Rule, in matchInput) *Rule {
	for i := range rules {
		if matchRule(&rules[i], in) {
			return &rules[i]
		}
	}
	return nil
}
