package categorizer

import (
	"fmt"
	"regexp"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/errormatch"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/metrics"
)

var classificationDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: metrics.ArmadaExecutorMetricsPrefix + "error_classification_duration_seconds",
		Help: "Time taken to classify pod errors into categories",
		Buckets: []float64{
			0.0001, 0.0005, 0.001, // 100µs, 500µs, 1ms
			0.005, 0.01, 0.05, 0.1, // 5ms, 10ms, 50ms, 100ms
		},
	},
	[]string{"queue"},
)

type category struct {
	name  string
	rules []rule
}

type rule struct {
	containerName        string
	onExitCodes          *errormatch.ExitCodeMatcher
	onTerminationMessage *regexp.Regexp
	onConditions         []string
}

// Classifier evaluates pods against a set of category rules and returns
// the names of all matching categories.
type Classifier struct {
	categories []category
}

// NewClassifier validates config and compiles regex patterns.
// Returns an error if any regex is invalid, a condition is unknown,
// or an exit code matcher has an invalid operator.
func NewClassifier(configs []CategoryConfig) (*Classifier, error) {
	categories := make([]category, 0, len(configs))
	seen := make(map[string]bool, len(configs))
	for _, cfg := range configs {
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
		for i, rule := range cfg.Rules {
			r, err := buildRule(rule)
			if err != nil {
				return nil, fmt.Errorf("category %q rule %d: %w", cfg.Name, i, err)
			}
			cat.rules = append(cat.rules, r)
		}
		categories = append(categories, cat)
	}
	return &Classifier{categories: categories}, nil
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
	}, nil
}

// Classify returns the names of all categories that match the given pod.
// Returns nil if the receiver is nil, the pod is nil, or no categories match.
func (c *Classifier) Classify(pod *v1.Pod) []string {
	if c == nil || pod == nil {
		return nil
	}

	start := time.Now()
	containers := failedContainers(pod)
	podReason := pod.Status.Reason

	var matched []string
	for _, cat := range c.categories {
		if categoryMatches(cat, containers, podReason) {
			matched = append(matched, cat.name)
		}
	}

	classificationDuration.WithLabelValues(pod.Labels[domain.Queue]).Observe(time.Since(start).Seconds())

	return matched
}

func categoryMatches(cat category, containers []containerInfo, podReason string) bool {
	for _, rule := range cat.rules {
		if ruleMatches(rule, containers, podReason) {
			return true
		}
	}
	return false
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
			// Container-level: check the terminated reason on each failed container.
			for _, c := range containers {
				if c.reason == cond {
					return true
				}
			}
		case errormatch.ConditionEvicted, errormatch.ConditionDeadlineExceeded:
			// Pod-level: these conditions appear on pod.Status.Reason, not on containers.
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
