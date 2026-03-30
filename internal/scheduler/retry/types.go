package retry

import (
	"fmt"
	"regexp"

	"github.com/armadaproject/armada/internal/common/errormatch"
)

// Action is the decision made by the retry engine.
type Action string

const (
	ActionFail  Action = "Fail"
	ActionRetry Action = "Retry"
)

// Policy is the internal representation used by the engine.
// Converted from the api.RetryPolicy proto at cache refresh time (in a later PR).
type Policy struct {
	Name          string
	RetryLimit    uint32
	DefaultAction Action
	Rules         []Rule
}

// Rule defines a single matching rule within a policy.
// All non-nil match fields must match for the rule to apply (AND logic across fields).
// OnCategories uses OR within the list: the rule matches if the job has any of the listed categories.
type Rule struct {
	Action               Action
	OnConditions         []string
	OnExitCodes          *errormatch.ExitCodeMatcher
	OnTerminationMessage *errormatch.RegexMatcher
	OnCategories         []string

	// compiledPattern holds the pre-compiled regex from OnTerminationMessage.
	// Populated by CompileRules; nil when OnTerminationMessage is nil.
	compiledPattern *regexp.Regexp
}

// Result is the output of the retry engine evaluation.
type Result struct {
	ShouldRetry bool
	Reason      string
}

// ValidatePolicy checks that a policy has valid fields.
func ValidatePolicy(p Policy) error {
	if p.DefaultAction != ActionFail && p.DefaultAction != ActionRetry {
		return fmt.Errorf("DefaultAction must be %q or %q, got %q", ActionFail, ActionRetry, p.DefaultAction)
	}
	return CompileRules(p.Rules)
}

// CompileRules validates and compiles regex patterns in all rules so they are ready for matching.
// Call this once when policies are loaded, not on every evaluation.
func CompileRules(rules []Rule) error {
	for i := range rules {
		if err := validateRule(i, rules[i]); err != nil {
			return err
		}
		if rules[i].OnTerminationMessage != nil {
			re, err := regexp.Compile(rules[i].OnTerminationMessage.Pattern)
			if err != nil {
				return fmt.Errorf("failed to compile termination message pattern %q in rule %d: %w",
					rules[i].OnTerminationMessage.Pattern, i, err)
			}
			rules[i].compiledPattern = re
		}
	}
	return nil
}

// validateRule checks that a rule has at least one match field and that
// configured matchers have valid parameters.
func validateRule(index int, rule Rule) error {
	hasConditions := len(rule.OnConditions) > 0
	hasExitCodes := rule.OnExitCodes != nil
	hasTermMsg := rule.OnTerminationMessage != nil
	hasCategories := len(rule.OnCategories) > 0

	if !hasConditions && !hasExitCodes && !hasTermMsg && !hasCategories {
		return fmt.Errorf("rule %d: must have at least one match field (OnConditions, OnExitCodes, OnTerminationMessage, or OnCategories)", index)
	}

	if hasExitCodes {
		op := rule.OnExitCodes.Operator
		if op != errormatch.ExitCodeOperatorIn && op != errormatch.ExitCodeOperatorNotIn {
			return fmt.Errorf("rule %d: OnExitCodes operator must be %q or %q, got %q",
				index, errormatch.ExitCodeOperatorIn, errormatch.ExitCodeOperatorNotIn, op)
		}
		if len(rule.OnExitCodes.Values) == 0 {
			return fmt.Errorf("rule %d: OnExitCodes values must not be empty", index)
		}
	}

	if hasTermMsg && rule.OnTerminationMessage.Pattern == "" {
		return fmt.Errorf("rule %d: OnTerminationMessage pattern must not be empty", index)
	}

	return nil
}
