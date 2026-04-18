package categorizer

import (
	"fmt"
	"regexp"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/errormatch"
)

// DefaultCategoryName is the category assigned when no classifier rule matches
// and no custom defaultCategory is configured.
const DefaultCategoryName = "uncategorized"

type category struct {
	name  string
	rules []rule
}

type rule struct {
	containerName        string
	onExitCodes          *errormatch.ExitCodeMatcher
	onTerminationMessage *regexp.Regexp
	onConditions         []string
	subcategory          string
}

// ClassifyResult holds the classification output for a failed pod.
type ClassifyResult struct {
	Category    string
	Subcategory string
}

// Classifier evaluates pods against a set of category rules and returns
// the first matching category and subcategory.
type Classifier struct {
	defaultCategory string
	categories      []category
}

// NewClassifier validates config and compiles regex patterns.
// Returns an error if any regex is invalid, a condition is unknown,
// or an exit code matcher has an invalid operator.
func NewClassifier(config ErrorCategoriesConfig) (*Classifier, error) {
	defaultCat := config.DefaultCategory
	if defaultCat == "" {
		defaultCat = DefaultCategoryName
	}

	categories := make([]category, 0, len(config.Categories))
	seen := make(map[string]bool, len(config.Categories))
	for _, cfg := range config.Categories {
		if cfg.Name == "" {
			return nil, fmt.Errorf("category config must have a name")
		}
		if seen[cfg.Name] {
			return nil, fmt.Errorf("duplicate category name %q", cfg.Name)
		}
		seen[cfg.Name] = true
		if len(cfg.Rules) == 0 {
			return nil, fmt.Errorf("category %q must have at least one rule", cfg.Name)
		}
		cat := category{name: cfg.Name}
		for i, r := range cfg.Rules {
			built, err := buildRule(r)
			if err != nil {
				return nil, fmt.Errorf("category %q rule %d: %w", cfg.Name, i, err)
			}
			cat.rules = append(cat.rules, built)
		}
		categories = append(categories, cat)
	}
	return &Classifier{defaultCategory: defaultCat, categories: categories}, nil
}

func buildRule(cfg CategoryRule) (rule, error) {
	matcherCount := 0
	if len(cfg.OnConditions) > 0 {
		matcherCount++
	}
	if cfg.OnExitCodes != nil {
		matcherCount++
	}
	if cfg.OnTerminationMessage != nil {
		matcherCount++
	}
	if matcherCount == 0 {
		return rule{}, fmt.Errorf("rule must specify one of onConditions, onExitCodes, or onTerminationMessage")
	}
	if matcherCount > 1 {
		return rule{}, fmt.Errorf("rule must specify only one of onConditions, onExitCodes, or onTerminationMessage")
	}

	for _, cond := range cfg.OnConditions {
		if !errormatch.KnownConditions[cond] {
			return rule{}, fmt.Errorf("unknown condition %q, valid values: %s, %s, %s",
				cond, errormatch.ConditionOOMKilled, errormatch.ConditionEvicted, errormatch.ConditionDeadlineExceeded)
		}
	}

	if cfg.OnExitCodes != nil {
		switch cfg.OnExitCodes.Operator {
		case errormatch.ExitCodeOperatorIn, errormatch.ExitCodeOperatorNotIn:
			// valid
		default:
			return rule{}, fmt.Errorf("invalid exit code operator %q, must be %q or %q",
				cfg.OnExitCodes.Operator, errormatch.ExitCodeOperatorIn, errormatch.ExitCodeOperatorNotIn)
		}
		if len(cfg.OnExitCodes.Values) == 0 {
			return rule{}, fmt.Errorf("exit code matcher requires at least one value")
		}
	}

	var compiledRegex *regexp.Regexp
	if cfg.OnTerminationMessage != nil {
		re, err := regexp.Compile(cfg.OnTerminationMessage.Pattern)
		if err != nil {
			return rule{}, fmt.Errorf("invalid regex %q: %w", cfg.OnTerminationMessage.Pattern, err)
		}
		compiledRegex = re
	}

	return rule{
		containerName:        cfg.ContainerName,
		onExitCodes:          cfg.OnExitCodes,
		onConditions:         cfg.OnConditions,
		onTerminationMessage: compiledRegex,
		subcategory:          cfg.Subcategory,
	}, nil
}

// Classify returns the category and subcategory for the given pod.
// Rules are evaluated in config order; the first matching rule wins.
// Returns empty result if the receiver is nil or the pod is nil.
// Returns (defaultCategory, "") if no rules match.
func (c *Classifier) Classify(pod *v1.Pod) ClassifyResult {
	if c == nil || pod == nil {
		return ClassifyResult{}
	}
	containers := failedContainers(pod)
	podReason := pod.Status.Reason

	for _, cat := range c.categories {
		for _, r := range cat.rules {
			if ruleMatches(r, containers, podReason) {
				return ClassifyResult{Category: cat.name, Subcategory: r.subcategory}
			}
		}
	}
	return ClassifyResult{Category: c.getDefaultCategory()}
}

func (c *Classifier) getDefaultCategory() string {
	return c.defaultCategory
}

// ruleMatches evaluates a single rule. When containerName is set, only that
// container is considered. It checks the first non-nil matcher:
// conditions > exit codes > termination message.
func ruleMatches(r rule, containers []containerInfo, podReason string) bool {
	filtered := containers
	if r.containerName != "" {
		filtered = filterByName(containers, r.containerName)
	}
	if len(r.onConditions) > 0 {
		return matchesCondition(r.onConditions, filtered, podReason)
	}
	if r.onExitCodes != nil {
		return matchesExitCodes(r.onExitCodes, filtered)
	}
	if r.onTerminationMessage != nil {
		return matchesTerminationMessage(r.onTerminationMessage, filtered)
	}
	return false
}

func filterByName(containers []containerInfo, name string) []containerInfo {
	var result []containerInfo
	for _, c := range containers {
		if c.name == name {
			result = append(result, c)
		}
	}
	return result
}

func matchesCondition(conditions []string, containers []containerInfo, podReason string) bool {
	for _, cond := range conditions {
		switch cond {
		case errormatch.ConditionOOMKilled:
			for _, c := range containers {
				if c.reason == cond {
					return true
				}
			}
		case errormatch.ConditionEvicted, errormatch.ConditionDeadlineExceeded:
			if podReason == cond {
				return true
			}
		}
	}
	return false
}

func matchesExitCodes(matcher *errormatch.ExitCodeMatcher, containers []containerInfo) bool {
	for _, c := range containers {
		if errormatch.MatchExitCode(matcher, c.exitCode) {
			return true
		}
	}
	return false
}

func matchesTerminationMessage(re *regexp.Regexp, containers []containerInfo) bool {
	for _, c := range containers {
		if errormatch.MatchPattern(re, c.terminationMessage) {
			return true
		}
	}
	return false
}

// containerInfo holds the failure-relevant fields from a terminated container.
type containerInfo struct {
	name               string
	exitCode           int32
	reason             string
	terminationMessage string
}

// failedContainers extracts failure info from all terminated containers
// (both regular and init containers) that exited with a non-zero code.
// Containers that exited successfully (exit code 0) are excluded to avoid
// false-positive matches on their termination messages or reasons.
func failedContainers(pod *v1.Pod) []containerInfo {
	var result []containerInfo
	for _, statuses := range [2][]v1.ContainerStatus{
		pod.Status.ContainerStatuses,
		pod.Status.InitContainerStatuses,
	} {
		for _, cs := range statuses {
			if cs.State.Terminated == nil || cs.State.Terminated.ExitCode == 0 {
				continue
			}
			result = append(result, containerInfo{
				name:               cs.Name,
				exitCode:           cs.State.Terminated.ExitCode,
				reason:             cs.State.Terminated.Reason,
				terminationMessage: cs.State.Terminated.Message,
			})
		}
	}
	return result
}
