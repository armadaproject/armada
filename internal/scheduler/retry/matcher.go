package retry

import (
	"slices"

	"github.com/armadaproject/armada/internal/common/errormatch"
)

// matchRule returns true if all non-empty match fields on the rule match the given values.
// Empty/nil match fields are ignored (they match anything).
func matchRule(rule *Rule, condition string, exitCode int32, terminationMessage string, categories []string) bool {
	if len(rule.OnConditions) > 0 {
		if !slices.Contains(rule.OnConditions, condition) {
			return false
		}
	}
	if rule.OnExitCodes != nil {
		if !errormatch.MatchExitCode(rule.OnExitCodes, exitCode) {
			return false
		}
	}
	if rule.OnTerminationMessage != nil {
		// Fail closed if CompileRules was not called: an uncompiled pattern
		// must never vacuously match every error.
		if rule.compiledPattern == nil {
			return false
		}
		if !errormatch.MatchPattern(rule.compiledPattern, terminationMessage) {
			return false
		}
	}
	if len(rule.OnCategories) > 0 {
		if !slices.ContainsFunc(rule.OnCategories, func(c string) bool { return slices.Contains(categories, c) }) {
			return false
		}
	}
	return true
}

// matchRules iterates rules in order and returns a pointer to the first
// matching rule. Returns nil if no rule matches.
func matchRules(rules []Rule, condition string, exitCode int32, terminationMessage string, categories []string) *Rule {
	for i := range rules {
		if matchRule(&rules[i], condition, exitCode, terminationMessage, categories) {
			return &rules[i]
		}
	}
	return nil
}
