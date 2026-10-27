package retry

// matchInput bundles the signals a rule may match against.
type matchInput struct {
	category    string
	subcategory string
}

// matchRule returns true if the rule's category (and subcategory, if set)
// matches the given input.
func matchRule(rule *Rule, in matchInput) bool {
	if in.category != rule.OnCategory {
		return false
	}
	if rule.OnSubcategory != "" && in.subcategory != rule.OnSubcategory {
		return false
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
