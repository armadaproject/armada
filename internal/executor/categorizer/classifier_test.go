package categorizer

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/errormatch"
)

func TestClassify(t *testing.T) {
	tests := map[string]struct {
		config              ErrorCategoriesConfig
		pod                 *v1.Pod
		expectedCategory    string
		expectedSubcategory string
	}{
		"OOM condition matches": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "oom", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}, Subcategory: "kernel"},
				}},
			}},
			pod:                 podWithTerminatedContainer(137, errormatch.ConditionOOMKilled, ""),
			expectedCategory:    "oom",
			expectedSubcategory: "kernel",
		},
		"exit code In matches": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "cuda_error", Rules: []CategoryRule{
					{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{74, 75}}},
				}},
			}},
			pod:              podWithTerminatedContainer(74, "Error", ""),
			expectedCategory: "cuda_error",
		},
		"exit code NotIn matches": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "unexpected", Rules: []CategoryRule{
					{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorNotIn, Values: []int32{1, 2}}},
				}},
			}},
			pod:              podWithTerminatedContainer(42, "Error", ""),
			expectedCategory: "unexpected",
		},
		"termination message regex matches": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "gpu_error", Rules: []CategoryRule{
					{OnTerminationMessage: &errormatch.RegexMatcher{Pattern: "(?i)cuda.*error"}, Subcategory: "cuda"},
				}},
			}},
			pod:                 podWithTerminatedContainer(1, "Error", "CUDA memory error on device 0"),
			expectedCategory:    "gpu_error",
			expectedSubcategory: "cuda",
		},
		"first match wins across categories": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "oom", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}},
				}},
				{Name: "high_exit", Rules: []CategoryRule{
					{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{137}}},
				}},
			}},
			pod:              podWithTerminatedContainer(137, errormatch.ConditionOOMKilled, ""),
			expectedCategory: "oom", // first match wins, not both
		},
		"no match returns empty when no default set": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "oom", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}},
				}},
			}},
			pod:              podWithTerminatedContainer(1, "Error", "normal failure"),
			expectedCategory: "",
		},
		"custom default category": {
			config: ErrorCategoriesConfig{
				DefaultCategory: "other",
				Categories: []CategoryConfig{
					{Name: "oom", Rules: []CategoryRule{
						{OnConditions: []string{errormatch.ConditionOOMKilled}},
					}},
				},
			},
			pod:              podWithTerminatedContainer(1, "Error", "normal failure"),
			expectedCategory: "other",
		},
		"custom default category and subcategory": {
			config: ErrorCategoriesConfig{
				DefaultCategory:    "uncategorized",
				DefaultSubcategory: "unknown",
				Categories: []CategoryConfig{
					{Name: "oom", Rules: []CategoryRule{
						{OnConditions: []string{errormatch.ConditionOOMKilled}},
					}},
				},
			},
			pod:                 podWithTerminatedContainer(1, "Error", "normal failure"),
			expectedCategory:    "uncategorized",
			expectedSubcategory: "unknown",
		},
		"matching rule subcategory wins over default subcategory": {
			config: ErrorCategoriesConfig{
				DefaultCategory:    "uncategorized",
				DefaultSubcategory: "unknown",
				Categories: []CategoryConfig{
					{Name: "oom", Rules: []CategoryRule{
						{OnConditions: []string{errormatch.ConditionOOMKilled}, Subcategory: "kernel"},
					}},
				},
			},
			pod:                 podWithTerminatedContainer(137, errormatch.ConditionOOMKilled, ""),
			expectedCategory:    "oom",
			expectedSubcategory: "kernel",
		},
		"nil pod returns empty": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "oom", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}},
				}},
			}},
			pod: nil,
		},
		"init container is checked": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "init_fail", Rules: []CategoryRule{
					{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{1}}},
				}},
			}},
			pod: &v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodFailed,
					InitContainerStatuses: []v1.ContainerStatus{
						{
							Name: "init",
							State: v1.ContainerState{
								Terminated: &v1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"},
							},
						},
					},
				},
			},
			expectedCategory: "init_fail",
		},
		"evicted condition matches": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "evicted", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionEvicted}},
				}},
			}},
			pod: &v1.Pod{
				Status: v1.PodStatus{
					Phase:  v1.PodFailed,
					Reason: errormatch.ConditionEvicted,
				},
			},
			expectedCategory: "evicted",
		},
		"deadline exceeded condition matches": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "timeout", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionDeadlineExceeded}},
				}},
			}},
			pod: &v1.Pod{
				Status: v1.PodStatus{
					Phase:  v1.PodFailed,
					Reason: errormatch.ConditionDeadlineExceeded,
				},
			},
			expectedCategory: "timeout",
		},
		"rules within category are ORed": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "infra", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}},
					{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{137}}},
				}},
			}},
			pod:              podWithTerminatedContainer(137, "Error", ""),
			expectedCategory: "infra",
		},
		"exit code 0 container is skipped": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "msg_match", Rules: []CategoryRule{
					{OnTerminationMessage: &errormatch.RegexMatcher{Pattern: "success"}},
				}},
			}},
			pod: &v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodFailed,
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "sidecar",
							State: v1.ContainerState{
								Terminated: &v1.ContainerStateTerminated{
									ExitCode: 0,
									Reason:   "Completed",
									Message:  "success message",
								},
							},
						},
						{
							Name: "main",
							State: v1.ContainerState{
								Terminated: &v1.ContainerStateTerminated{
									ExitCode: 1,
									Reason:   "Error",
									Message:  "actual failure",
								},
							},
						},
					},
				},
			},
			expectedCategory: "",
		},
		"containerName targets specific container": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "main_oom", Rules: []CategoryRule{
					{ContainerName: "main", OnConditions: []string{errormatch.ConditionOOMKilled}},
				}},
			}},
			pod: &v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodFailed,
					ContainerStatuses: []v1.ContainerStatus{
						{Name: "sidecar", State: v1.ContainerState{
							Terminated: &v1.ContainerStateTerminated{ExitCode: 137, Reason: errormatch.ConditionOOMKilled},
						}},
						{Name: "main", State: v1.ContainerState{
							Terminated: &v1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"},
						}},
					},
				},
			},
			expectedCategory: "", // sidecar OOMs but main does not, rule targets main only
		},
		"containerName matches when container matches": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "main_error", Rules: []CategoryRule{
					{ContainerName: "main", OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{42}}},
				}},
			}},
			pod: &v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodFailed,
					ContainerStatuses: []v1.ContainerStatus{
						{Name: "main", State: v1.ContainerState{
							Terminated: &v1.ContainerStateTerminated{ExitCode: 42, Reason: "Error"},
						}},
					},
				},
			},
			expectedCategory: "main_error",
		},
		"no containerName matches any container": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "any_oom", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}},
				}},
			}},
			pod: &v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodFailed,
					ContainerStatuses: []v1.ContainerStatus{
						{Name: "sidecar", State: v1.ContainerState{
							Terminated: &v1.ContainerStateTerminated{ExitCode: 137, Reason: errormatch.ConditionOOMKilled},
						}},
					},
				},
			},
			expectedCategory: "any_oom",
		},
		"empty config returns empty": {
			config:           ErrorCategoriesConfig{},
			pod:              podWithTerminatedContainer(1, "Error", ""),
			expectedCategory: "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			classifier, err := NewClassifier(tc.config)
			require.NoError(t, err)
			result := classifier.Classify(tc.pod)
			assert.Equal(t, tc.expectedCategory, result.Category)
			assert.Equal(t, tc.expectedSubcategory, result.Subcategory)
		})
	}
}

func TestNewClassifier_ValidationErrors(t *testing.T) {
	tests := map[string]struct {
		config      ErrorCategoriesConfig
		errContains string
	}{
		"empty name": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}},
				}},
			}},
			errContains: "must have a name",
		},
		"empty rule": {
			config:      ErrorCategoriesConfig{Categories: []CategoryConfig{{Name: "bad", Rules: []CategoryRule{{}}}}},
			errContains: "must specify one of",
		},
		"multiple matchers in rule": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "bad", Rules: []CategoryRule{
					{
						OnConditions: []string{errormatch.ConditionOOMKilled},
						OnExitCodes:  &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{137}},
					},
				}},
			}},
			errContains: "must specify only one of",
		},
		"unknown condition": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "bad", Rules: []CategoryRule{
					{OnConditions: []string{"NotARealCondition"}},
				}},
			}},
			errContains: "unknown condition",
		},
		"invalid exit code operator": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "bad", Rules: []CategoryRule{
					{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: "Equals", Values: []int32{1}}},
				}},
			}},
			errContains: "invalid exit code operator",
		},
		"empty exit code values": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "bad", Rules: []CategoryRule{
					{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: nil}},
				}},
			}},
			errContains: "requires at least one value",
		},
		"invalid regex": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "bad", Rules: []CategoryRule{
					{OnTerminationMessage: &errormatch.RegexMatcher{Pattern: "[invalid"}},
				}},
			}},
			errContains: "invalid regex",
		},
		"empty rules": {
			config:      ErrorCategoriesConfig{Categories: []CategoryConfig{{Name: "empty", Rules: nil}}},
			errContains: "must have at least one rule",
		},
		"duplicate category name": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "oom", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}},
				}},
				{Name: "oom", Rules: []CategoryRule{
					{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{137}}},
				}},
			}},
			errContains: "duplicate category name",
		},
		"category name too long": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: strings.Repeat("a", maxCategoryNameLen+1), Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}},
				}},
			}},
			errContains: "category name",
		},
		"subcategory too long": {
			config: ErrorCategoriesConfig{Categories: []CategoryConfig{
				{Name: "oom", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}, Subcategory: strings.Repeat("b", maxCategoryNameLen+1)},
				}},
			}},
			errContains: "subcategory",
		},
		"default category too long": {
			config: ErrorCategoriesConfig{
				DefaultCategory: strings.Repeat("c", maxCategoryNameLen+1),
			},
			errContains: "defaultCategory",
		},
		"default subcategory too long": {
			config: ErrorCategoriesConfig{
				DefaultSubcategory: strings.Repeat("d", maxCategoryNameLen+1),
			},
			errContains: "defaultSubcategory",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := NewClassifier(tc.config)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errContains)
		})
	}
}

func TestClassify_RecordsRuleEvaluationDuration(t *testing.T) {
	classifier, err := NewClassifier(ErrorCategoriesConfig{Categories: []CategoryConfig{
		{Name: "infra", Rules: []CategoryRule{
			{OnConditions: []string{errormatch.ConditionOOMKilled}, Subcategory: "oom"},
		}},
		{Name: "user_error", Rules: []CategoryRule{
			{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{42}}, Subcategory: "bad_exit"},
			{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{74}}, Subcategory: "other_exit"},
		}},
	}})
	require.NoError(t, err)

	before := ruleHistogramCounts(t)
	pod := podWithTerminatedContainer(42, "Error", "")
	result := classifier.Classify(pod)
	require.Equal(t, "user_error", result.Category)
	require.Equal(t, "bad_exit", result.Subcategory)
	after := ruleHistogramCounts(t)

	// First rule (infra/oom) does not match -> observed.
	assert.Equal(t, uint64(1), after[labelKey{"infra", "oom"}]-before[labelKey{"infra", "oom"}],
		"non-matching rule before the match should be observed")
	// Second rule (user_error/bad_exit) matches -> observed.
	assert.Equal(t, uint64(1), after[labelKey{"user_error", "bad_exit"}]-before[labelKey{"user_error", "bad_exit"}],
		"matching rule should be observed")
	// Third rule (user_error/other_exit) is after the match -> not reached, not observed.
	assert.Equal(t, uint64(0), after[labelKey{"user_error", "other_exit"}]-before[labelKey{"user_error", "other_exit"}],
		"rule after the match should not be observed")
}

type labelKey struct{ category, subcategory string }

// ruleHistogramCounts gathers the rule-evaluation histogram from the default
// registry and returns a map of (category, subcategory) -> sample count.
func ruleHistogramCounts(t *testing.T) map[labelKey]uint64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	counts := map[labelKey]uint64{}
	for _, mf := range families {
		if mf.GetName() != "armada_executor_job_failure_rule_evaluation_duration_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			counts[labelKeyFromMetric(m)] = m.GetHistogram().GetSampleCount()
		}
	}
	return counts
}

func labelKeyFromMetric(m *dto.Metric) labelKey {
	var k labelKey
	for _, lp := range m.GetLabel() {
		switch lp.GetName() {
		case "failure_category":
			k.category = lp.GetValue()
		case "failure_subcategory":
			k.subcategory = lp.GetValue()
		}
	}
	return k
}

func podWithTerminatedContainer(exitCode int32, reason, message string) *v1.Pod {
	return &v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodFailed,
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name: "main",
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{
							ExitCode: exitCode,
							Reason:   reason,
							Message:  message,
						},
					},
				},
			},
		},
	}
}
