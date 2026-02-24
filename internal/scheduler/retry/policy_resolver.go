package retry

import (
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
)

// PolicyResolver resolves the retry policy for a given queue.
type PolicyResolver struct {
	config configuration.RetryPolicyConfig
	log    *logrus.Entry
}

// NewPolicyResolver creates a new PolicyResolver.
// It pre-compiles all regex patterns and validates rule configuration.
func NewPolicyResolver(config configuration.RetryPolicyConfig) (*PolicyResolver, error) {
	resolver := &PolicyResolver{
		config: config,
		log:    logrus.WithField("component", "PolicyResolver"),
	}

	if err := resolver.compileRegexPatterns(); err != nil {
		return nil, err
	}

	resolver.warnMultiMatcherRules()

	return resolver, nil
}

func (r *PolicyResolver) compileRegexPatterns() error {
	if err := compileRulesRegex(r.config.Default.Rules); err != nil {
		return err
	}
	for name, policy := range r.config.Policies {
		if err := compileRulesRegex(policy.Rules); err != nil {
			return fmt.Errorf("policy %q: %w", name, err)
		}
	}

	return nil
}

func compileRulesRegex(rules []configuration.Rule) error {
	for i := range rules {
		if rules[i].OnTerminationMessage != nil {
			if err := rules[i].OnTerminationMessage.Compile(); err != nil {
				return err
			}
		}
	}
	return nil
}

// warnMultiMatcherRules logs warnings for rules that have more than one matcher set.
// Only the first non-nil matcher is evaluated (conditions > exitCodes > terminationMessage).
func (r *PolicyResolver) warnMultiMatcherRules() {
	checkRules := func(policyName string, rules []configuration.Rule) {
		for i, rule := range rules {
			matcherCount := 0
			if len(rule.OnConditions) > 0 {
				matcherCount++
			}
			if rule.OnExitCodes != nil {
				matcherCount++
			}
			if rule.OnTerminationMessage != nil {
				matcherCount++
			}
			if matcherCount > 1 {
				r.log.WithFields(logrus.Fields{
					"policy":    policyName,
					"ruleIndex": i,
				}).Warn("Rule has multiple matchers; only the first will be evaluated (conditions > exitCodes > terminationMessage)")
			}
		}
	}
	checkRules("default", r.config.Default.Rules)
	for name, policy := range r.config.Policies {
		checkRules(name, policy.Rules)
	}
}

func (r *PolicyResolver) Enabled() bool {
	return r.config.Enabled
}

func (r *PolicyResolver) GlobalMaxRetries() uint {
	return r.config.GlobalMaxRetries
}

// GetPolicy returns the named policy, or default if not found or not specified.
func (r *PolicyResolver) GetPolicy(queueRetryPolicy string) *configuration.Policy {
	if !r.config.Enabled {
		return nil
	}

	if queueRetryPolicy != "" {
		if policy, exists := r.config.Policies[queueRetryPolicy]; exists {
			return &policy
		}
		r.log.WithFields(logrus.Fields{
			"policy": queueRetryPolicy,
		}).Warn("Queue references unknown retry policy, using default")
	}

	return &r.config.Default
}
