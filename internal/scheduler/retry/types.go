package retry

import (
	"fmt"
)

// Action is what a rule or a policy's default prescribes for a failed run:
// retry it or fail it permanently.
type Action string

const (
	ActionFail  Action = "Fail"
	ActionRetry Action = "Retry"
)

// Policy is the internal representation used by the engine.
// Converted from the api.RetryPolicy proto at cache refresh time.
type Policy struct {
	Name          string
	RetryLimit    uint32
	DefaultAction Action
	Rules         []Rule
}

// Rule defines a single matching rule within a policy.
// OnSubcategory narrows a category match. Empty matches any subcategory.
type Rule struct {
	Action        Action
	OnCategory    string
	OnSubcategory string
}

// Decision identifies which gate produced an engine verdict. It is used as
// a prometheus label value, so it must stay low-cardinality.
type Decision string

const (
	DecisionRetry           Decision = "retry"
	DecisionFailRule        Decision = "fail_rule"
	DecisionFailPolicyLimit Decision = "fail_policy_limit"
	DecisionFailGlobalLimit Decision = "fail_global_limit"
	DecisionFailDefault     Decision = "fail_default"
	// DecisionNoError is returned when there is no run error to evaluate. It
	// gives the nil-error path a non-empty metrics label instead of a gap.
	DecisionNoError Decision = "no_error"
)

// Result is the output of the retry engine evaluation.
type Result struct {
	ShouldRetry bool
	Reason      string
	// Decision is the typed counterpart of Reason, suitable for metrics. It is
	// always set.
	Decision Decision
}

// ValidatePolicy checks that a policy has valid fields.
func ValidatePolicy(p Policy) error {
	if p.Name == "" {
		return fmt.Errorf("policy name must not be empty")
	}
	if p.DefaultAction != ActionFail && p.DefaultAction != ActionRetry {
		return fmt.Errorf("DefaultAction must be %q or %q, got %q", ActionFail, ActionRetry, p.DefaultAction)
	}
	for i := range p.Rules {
		if err := validateRule(i, p.Rules[i]); err != nil {
			return err
		}
	}
	return nil
}

func validateRule(index int, rule Rule) error {
	if rule.Action != ActionFail && rule.Action != ActionRetry {
		return fmt.Errorf("rule %d: Action must be %q or %q, got %q", index, ActionFail, ActionRetry, rule.Action)
	}
	if rule.OnCategory == "" {
		return fmt.Errorf("rule %d: OnCategory must be set", index)
	}
	return nil
}
