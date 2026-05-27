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
// All non-empty match fields must match for the rule to apply (AND logic across fields).
// OnSubcategory is only valid when OnCategory is set; it narrows a category match.
type Rule struct {
	Action               Action
	OnConditions         []string
	OnExitCodes          *errormatch.ExitCodeMatcher
	OnTerminationMessage *errormatch.RegexMatcher
	OnCategory           string
	OnSubcategory        string

	// compiledTerminationMessage holds the pre-compiled regex from OnTerminationMessage.
	// Populated by CompileRules; nil when OnTerminationMessage is nil.
	compiledTerminationMessage *regexp.Regexp
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

// CompileRules validates each rule and compiles its OnTerminationMessage
// regex, if present. Call once when a policy is loaded, not on every evaluation.
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
			rules[i].compiledTerminationMessage = re
		}
	}
	return nil
}

func validateRule(index int, rule Rule) error {
	if rule.Action != ActionFail && rule.Action != ActionRetry {
		return fmt.Errorf("rule %d: Action must be %q or %q, got %q", index, ActionFail, ActionRetry, rule.Action)
	}

	hasCategory := rule.OnCategory != ""
	hasMatcher := len(rule.OnConditions) > 0 || rule.OnExitCodes != nil || rule.OnTerminationMessage != nil || hasCategory
	if !hasMatcher {
		return fmt.Errorf("rule %d: must have at least one match field (OnConditions, OnExitCodes, OnTerminationMessage, or OnCategory)", index)
	}
	if rule.OnSubcategory != "" && !hasCategory {
		return fmt.Errorf("rule %d: OnSubcategory requires OnCategory to be set", index)
	}

	if rule.OnExitCodes != nil {
		op := rule.OnExitCodes.Operator
		if op != errormatch.ExitCodeOperatorIn && op != errormatch.ExitCodeOperatorNotIn {
			return fmt.Errorf("rule %d: OnExitCodes operator must be %q or %q, got %q",
				index, errormatch.ExitCodeOperatorIn, errormatch.ExitCodeOperatorNotIn, op)
		}
		if len(rule.OnExitCodes.Values) == 0 {
			return fmt.Errorf("rule %d: OnExitCodes values must not be empty", index)
		}
	}

	if rule.OnTerminationMessage != nil && rule.OnTerminationMessage.Pattern == "" {
		return fmt.Errorf("rule %d: OnTerminationMessage pattern must not be empty", index)
	}

	return nil
}
