package categorizer

import (
	"fmt"
	"regexp"
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/errormatch"
	"github.com/armadaproject/armada/internal/executor/metrics"
)

// maxCategoryNameLen is the maximum length for category and subcategory strings.
// Matches the varchar(63) constraint on the failure_category and failure_subcategory
// columns in the lookout job_run table (migration 032). Validating at config load
// time prevents the ingester from failing batch UPDATEs at runtime.
const maxCategoryNameLen = 63

type category struct {
	name  string
	rules []rule
}

type rule struct {
	containerName        string
	onExitCodes          *errormatch.ExitCodeMatcher
	onTerminationMessage *regexp.Regexp
	onPodError           *regexp.Regexp
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
	defaultCategory    string
	defaultSubcategory string
	categories         []category
}

// NewClassifier validates config and compiles regex patterns.
// Returns an error if any regex is invalid, a condition is unknown,
// or an exit code matcher has an invalid operator.
func NewClassifier(config ErrorCategoriesConfig) (*Classifier, error) {
	if len(config.DefaultCategory) > maxCategoryNameLen {
		return nil, fmt.Errorf("defaultCategory %q exceeds maximum length %d", config.DefaultCategory, maxCategoryNameLen)
	}
	if len(config.DefaultSubcategory) > maxCategoryNameLen {
		return nil, fmt.Errorf("defaultSubcategory %q exceeds maximum length %d", config.DefaultSubcategory, maxCategoryNameLen)
	}
	categories := make([]category, 0, len(config.Categories))
	seen := make(map[string]bool, len(config.Categories))
	for _, cfg := range config.Categories {
		if cfg.Name == "" {
			return nil, fmt.Errorf("category config must have a name")
		}
		if len(cfg.Name) > maxCategoryNameLen {
			return nil, fmt.Errorf("category name %q exceeds maximum length %d", cfg.Name, maxCategoryNameLen)
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
	return &Classifier{
		defaultCategory:    config.DefaultCategory,
		defaultSubcategory: config.DefaultSubcategory,
		categories:         categories,
	}, nil
}

func buildRule(cfg CategoryRule) (rule, error) {
	if len(cfg.Subcategory) > maxCategoryNameLen {
		return rule{}, fmt.Errorf("subcategory %q exceeds maximum length %d", cfg.Subcategory, maxCategoryNameLen)
	}
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
	if cfg.OnPodError != nil {
		matcherCount++
	}
	if matcherCount == 0 {
		return rule{}, fmt.Errorf("rule must specify one of onConditions, onExitCodes, onTerminationMessage, or onPodError")
	}
	if matcherCount > 1 {
		return rule{}, fmt.Errorf("rule must specify only one of onConditions, onExitCodes, onTerminationMessage, or onPodError")
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

	var terminationRegex *regexp.Regexp
	if cfg.OnTerminationMessage != nil {
		re, err := regexp.Compile(cfg.OnTerminationMessage.Pattern)
		if err != nil {
			return rule{}, fmt.Errorf("invalid onTerminationMessage regex %q: %w", cfg.OnTerminationMessage.Pattern, err)
		}
		terminationRegex = re
	}

	var podErrorRegex *regexp.Regexp
	if cfg.OnPodError != nil {
		re, err := regexp.Compile(cfg.OnPodError.Pattern)
		if err != nil {
			return rule{}, fmt.Errorf("invalid onPodError regex %q: %w", cfg.OnPodError.Pattern, err)
		}
		podErrorRegex = re
	}

	return rule{
		containerName:        cfg.ContainerName,
		onExitCodes:          cfg.OnExitCodes,
		onConditions:         cfg.OnConditions,
		onTerminationMessage: terminationRegex,
		onPodError:           podErrorRegex,
		subcategory:          cfg.Subcategory,
	}, nil
}

// ClassifyContainerError returns the category and subcategory for a pod whose
// failure is described by its own state: terminated containers, exit codes,
// and Kubernetes conditions. Use it for terminated pods (PodFailed phase).
// Returns empty result if the receiver is nil or the pod is nil.
// Returns (defaultCategory, defaultSubcategory) if no rules match.
func (c *Classifier) ClassifyContainerError(pod *v1.Pod) ClassifyResult {
	return c.classify(pod, "")
}

// ClassifyPodError returns the category and subcategory for a pod-level failure
// captured by the executor (image pull, missing volume, stuck terminating,
// active deadline exceeded, etc.). It additionally matches podErrorMessage
// against onPodError rules (see CategoryRule.OnPodError); all other rule types
// are evaluated against pod state, preserving first-match-wins across config order.
// Returns empty result if the receiver is nil or the pod is nil.
// Returns (defaultCategory, defaultSubcategory) if no rules match.
func (c *Classifier) ClassifyPodError(pod *v1.Pod, podErrorMessage string) ClassifyResult {
	return c.classify(pod, podErrorMessage)
}

// Rules are evaluated in config order; the first matching rule wins.
func (c *Classifier) classify(pod *v1.Pod, podErrorMessage string) ClassifyResult {
	if c == nil || pod == nil {
		return ClassifyResult{}
	}
	containers := failedContainers(pod)
	podReason := pod.Status.Reason

	for _, cat := range c.categories {
		for _, r := range cat.rules {
			start := time.Now()
			matched := ruleMatches(r, containers, podReason, podErrorMessage)
			metrics.RecordRuleEvaluationDuration(cat.name, r.subcategory, time.Since(start))
			if matched {
				return ClassifyResult{Category: cat.name, Subcategory: r.subcategory}
			}
		}
	}
	return ClassifyResult{Category: c.defaultCategory, Subcategory: c.defaultSubcategory}
}

// ruleMatches evaluates a single rule. Container-level matchers honor the
// rule's containerName scope (when set); onPodError ignores it because the
// pod-level error has no container attribution. Exactly one matcher is set
// per rule (validated at NewClassifier).
func ruleMatches(r rule, containers []containerInfo, podReason, podErrorMessage string) bool {
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
	if r.onPodError != nil {
		return podErrorMessage != "" && errormatch.MatchPattern(r.onPodError, podErrorMessage)
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
