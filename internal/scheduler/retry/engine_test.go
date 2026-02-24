package retry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestEngine_Disabled(t *testing.T) {
	engine, err := NewEngine(configuration.RetryPolicyConfig{
		Enabled: false,
	})
	require.NoError(t, err)

	assert.False(t, engine.Enabled())

	// When disabled, should fall back to pod check decision
	result := engine.Evaluate("", &armadaevents.FailureInfo{
		PodCheckRetryable: true,
	}, 0, 1)

	assert.Equal(t, configuration.ActionRetry, result.Action)
	assert.True(t, result.ShouldRequeue)
	assert.True(t, result.IncrementFailureCount)
}

func TestEngine_GlobalMaxRetriesZeroMeansNoRetries(t *testing.T) {
	// GlobalMaxRetries = 0 means no retries allowed (only 1 attempt)
	engine, err := NewEngine(configuration.RetryPolicyConfig{
		Enabled:          true,
		GlobalMaxRetries: 0, // Zero retries = 1 attempt only
		Default: configuration.Policy{
			RetryLimit: 3,
			Rules: []configuration.Rule{
				{
					Action: configuration.ActionRetry,
					OnExitCodes: &configuration.ExitCodeMatcher{
						Operator: "NotIn",
						Values:   []int32{0},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// First run fails (totalRuns = 1), globalMaxRetries = 0
	// Check: 1 > 0 = true → no retry allowed
	result := engine.Evaluate("", &armadaevents.FailureInfo{
		ExitCode: 1,
	}, 0, 1)

	assert.Equal(t, configuration.ActionFail, result.Action)
	assert.False(t, result.ShouldRequeue)
	assert.False(t, result.IncrementFailureCount)
}

func TestEngine_DefaultNoRulesImpliesFail(t *testing.T) {
	engine, err := NewEngine(configuration.RetryPolicyConfig{
		Enabled:          true,
		GlobalMaxRetries: 20,
		Default: configuration.Policy{
			RetryLimit: 3,
			Rules:      []configuration.Rule{}, // No rules = everything fails
		},
	})
	require.NoError(t, err)

	result := engine.Evaluate("", &armadaevents.FailureInfo{
		ExitCode:  1,
		Condition: armadaevents.FailureCondition_FAILURE_CONDITION_USER_ERROR,
	}, 0, 1)

	assert.Equal(t, configuration.ActionFail, result.Action)
	assert.False(t, result.ShouldRequeue)
	assert.False(t, result.IncrementFailureCount)
}

func TestEngine_RetryOOM(t *testing.T) {
	exitCode := int32(137)
	engine, err := NewEngine(configuration.RetryPolicyConfig{
		Enabled:          true,
		GlobalMaxRetries: 20,
		Default: configuration.Policy{
			RetryLimit: 3,
			Rules: []configuration.Rule{
				{
					Action: configuration.ActionRetry,
					OnExitCodes: &configuration.ExitCodeMatcher{
						Operator: "In",
						Values:   []int32{137},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	result := engine.Evaluate("", &armadaevents.FailureInfo{
		ExitCode:  exitCode,
		Condition: armadaevents.FailureCondition_FAILURE_CONDITION_OOM_KILLED,
	}, 0, 1)

	assert.Equal(t, configuration.ActionRetry, result.Action)
	assert.True(t, result.ShouldRequeue)
	assert.True(t, result.IncrementFailureCount)
}

func TestEngine_RetryLimitExceeded(t *testing.T) {
	exitCode := int32(137)
	engine, err := NewEngine(configuration.RetryPolicyConfig{
		Enabled:          true,
		GlobalMaxRetries: 20,
		Default: configuration.Policy{
			RetryLimit: 3,
			Rules: []configuration.Rule{
				{
					Action: configuration.ActionRetry,
					OnExitCodes: &configuration.ExitCodeMatcher{
						Operator: "In",
						Values:   []int32{137},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Already at retry limit
	result := engine.Evaluate("", &armadaevents.FailureInfo{
		ExitCode:  exitCode,
		Condition: armadaevents.FailureCondition_FAILURE_CONDITION_OOM_KILLED,
	}, 3, 3) // failureCount = 3, retryLimit = 3

	assert.Equal(t, configuration.ActionFail, result.Action)
	assert.False(t, result.ShouldRequeue)
	assert.False(t, result.IncrementFailureCount)
}

func TestEngine_RetryLimitZeroNeverRetries(t *testing.T) {
	// RetryLimit = 0 means the rule matches but retries are never allowed.
	// currentFailureCount (0) >= retryLimit (0) is true on the very first failure.
	engine, err := NewEngine(configuration.RetryPolicyConfig{
		Enabled:          true,
		GlobalMaxRetries: 20,
		Default: configuration.Policy{
			RetryLimit: 0,
			Rules: []configuration.Rule{
				{
					Action: configuration.ActionRetry,
					OnExitCodes: &configuration.ExitCodeMatcher{
						Operator: "NotIn",
						Values:   []int32{0},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Rule matches (exit code 1, NotIn [0]), but retryLimit=0 blocks the retry
	result := engine.Evaluate("", &armadaevents.FailureInfo{
		ExitCode: 1,
	}, 0, 1)

	assert.Equal(t, configuration.ActionFail, result.Action)
	assert.False(t, result.ShouldRequeue)
	assert.False(t, result.IncrementFailureCount)
}

func TestEngine_GlobalMaxRetriesExceeded(t *testing.T) {
	// globalMaxRetries: 5 means 5 retries allowed (6 total runs max)
	engine, err := NewEngine(configuration.RetryPolicyConfig{
		Enabled:          true,
		GlobalMaxRetries: 5,
		Default: configuration.Policy{
			RetryLimit: 10,
			Rules: []configuration.Rule{
				{
					Action: configuration.ActionRetry,
					OnExitCodes: &configuration.ExitCodeMatcher{
						Operator: "NotIn",
						Values:   []int32{0},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	assert.Equal(t, uint(5), engine.GlobalMaxRetries())

	// totalRuns = 5, globalMaxRetries = 5
	// Check: 5 > 5 = false → can still retry (one more allowed)
	result := engine.Evaluate("", &armadaevents.FailureInfo{
		ExitCode: 1,
	}, 0, 5)
	assert.Equal(t, configuration.ActionRetry, result.Action)
	assert.True(t, result.ShouldRequeue)
	assert.True(t, result.IncrementFailureCount)

	// totalRuns = 6, globalMaxRetries = 5
	// Check: 6 > 5 = true → no more retries
	result = engine.Evaluate("", &armadaevents.FailureInfo{
		ExitCode: 1,
	}, 0, 6)
	assert.Equal(t, configuration.ActionFail, result.Action)
	assert.False(t, result.ShouldRequeue)
	assert.False(t, result.IncrementFailureCount)
}

func TestEngine_NoMatchImpliesFail(t *testing.T) {
	exitCode := int32(1)
	engine, err := NewEngine(configuration.RetryPolicyConfig{
		Enabled:          true,
		GlobalMaxRetries: 20,
		Default: configuration.Policy{
			RetryLimit: 3,
			Rules: []configuration.Rule{
				// Only retry exit code 137
				{
					Action: configuration.ActionRetry,
					OnExitCodes: &configuration.ExitCodeMatcher{
						Operator: "In",
						Values:   []int32{137},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Exit code 1 doesn't match any rule
	result := engine.Evaluate("", &armadaevents.FailureInfo{
		ExitCode:  exitCode,
		Condition: armadaevents.FailureCondition_FAILURE_CONDITION_USER_ERROR,
	}, 0, 1)

	assert.Equal(t, configuration.ActionFail, result.Action)
	assert.False(t, result.ShouldRequeue)
	assert.False(t, result.IncrementFailureCount)
}

func TestEngine_NamedPolicy(t *testing.T) {
	exitCode := int32(137)
	engine, err := NewEngine(configuration.RetryPolicyConfig{
		Enabled:          true,
		GlobalMaxRetries: 20,
		Default: configuration.Policy{
			RetryLimit: 1, // Low limit
			Rules:      []configuration.Rule{},
		},
		Policies: map[string]configuration.Policy{
			"ml-training": {
				RetryLimit: 10, // Higher limit for ML
				Rules: []configuration.Rule{
					{
						Action: configuration.ActionRetry,
						OnExitCodes: &configuration.ExitCodeMatcher{
							Operator: "In",
							Values:   []int32{137},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Using named policy with higher limit
	result := engine.Evaluate("ml-training", &armadaevents.FailureInfo{
		ExitCode: exitCode,
	}, 5, 5) // failureCount = 5, which exceeds default but not ml-training

	assert.Equal(t, configuration.ActionRetry, result.Action)
	assert.True(t, result.ShouldRequeue)
	assert.True(t, result.IncrementFailureCount)
}

func TestEngine_MissingPolicyFallsBackToDefault(t *testing.T) {
	exitCode := int32(1)
	engine, err := NewEngine(configuration.RetryPolicyConfig{
		Enabled:          true,
		GlobalMaxRetries: 20,
		Default: configuration.Policy{
			RetryLimit: 3,
			Rules:      []configuration.Rule{}, // No rules = fail
		},
		Policies: map[string]configuration.Policy{}, // No policies defined
	})
	require.NoError(t, err)

	// Reference non-existent policy -> falls back to default
	result := engine.Evaluate("non-existent-policy", &armadaevents.FailureInfo{
		ExitCode: exitCode,
	}, 0, 1)

	// Default has no rules, so implicit Fail
	assert.Equal(t, configuration.ActionFail, result.Action)
	assert.False(t, result.ShouldRequeue)
	assert.False(t, result.IncrementFailureCount)
}

func TestMatcher_ExitCodeNotIn(t *testing.T) {
	matcher := NewMatcher()
	exitCode := int32(1)

	rules := []configuration.Rule{
		{
			Action: configuration.ActionRetry,
			OnExitCodes: &configuration.ExitCodeMatcher{
				Operator: "NotIn",
				Values:   []int32{0}, // Retry any non-zero
			},
		},
	}

	action := matcher.Match(&armadaevents.FailureInfo{
		ExitCode: exitCode,
	}, rules)

	assert.Equal(t, configuration.ActionRetry, action)
}

func TestMatcher_ExitCodeIn(t *testing.T) {
	matcher := NewMatcher()
	exitCode := int32(137)

	rules := []configuration.Rule{
		{
			Action: configuration.ActionRetry,
			OnExitCodes: &configuration.ExitCodeMatcher{
				Operator: "In",
				Values:   []int32{137, 143},
			},
		},
	}

	action := matcher.Match(&armadaevents.FailureInfo{
		ExitCode: exitCode,
	}, rules)

	assert.Equal(t, configuration.ActionRetry, action)
}

func TestMatcher_ConditionMatch(t *testing.T) {
	matcher := NewMatcher()

	rules := []configuration.Rule{
		{
			Action: configuration.ActionRetry,
			OnConditions: []configuration.FailureCondition{
				configuration.ConditionOOMKilled,
			},
		},
	}

	// Test OOMKilled matches
	action := matcher.Match(&armadaevents.FailureInfo{
		Condition: armadaevents.FailureCondition_FAILURE_CONDITION_OOM_KILLED,
	}, rules)
	assert.Equal(t, configuration.ActionRetry, action)

	// Test Evicted doesn't match
	action = matcher.Match(&armadaevents.FailureInfo{
		Condition: armadaevents.FailureCondition_FAILURE_CONDITION_EVICTED,
	}, rules)
	assert.Equal(t, configuration.ActionFail, action) // No match -> implicit Fail
}

func TestEngine_RetryOnPreemption(t *testing.T) {
	engine, err := NewEngine(configuration.RetryPolicyConfig{
		Enabled:          true,
		GlobalMaxRetries: 20,
		Default: configuration.Policy{
			RetryLimit: 5,
			Rules: []configuration.Rule{
				{
					Action: configuration.ActionRetry,
					OnConditions: []configuration.FailureCondition{
						configuration.ConditionPreempted,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Test preemption triggers retry
	result := engine.Evaluate("", &armadaevents.FailureInfo{
		Condition: armadaevents.FailureCondition_FAILURE_CONDITION_PREEMPTED,
	}, 0, 1)

	assert.Equal(t, configuration.ActionRetry, result.Action)
	assert.True(t, result.ShouldRequeue)
	assert.True(t, result.IncrementFailureCount)
}

func TestEngine_PreemptionRetryLimitExceeded(t *testing.T) {
	engine, err := NewEngine(configuration.RetryPolicyConfig{
		Enabled:          true,
		GlobalMaxRetries: 20,
		Default: configuration.Policy{
			RetryLimit: 3,
			Rules: []configuration.Rule{
				{
					Action: configuration.ActionRetry,
					OnConditions: []configuration.FailureCondition{
						configuration.ConditionPreempted,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Test preemption at retry limit fails
	result := engine.Evaluate("", &armadaevents.FailureInfo{
		Condition: armadaevents.FailureCondition_FAILURE_CONDITION_PREEMPTED,
	}, 3, 3) // failureCount = 3 = retryLimit

	assert.Equal(t, configuration.ActionFail, result.Action)
	assert.False(t, result.ShouldRequeue)
	assert.False(t, result.IncrementFailureCount)
}

func TestMatcher_TerminationMessageRegex(t *testing.T) {
	matcher := NewMatcher()

	regexMatcher := &configuration.RegexMatcher{
		Pattern: "TRANSIENT_ERROR|RETRY_ME",
	}
	require.NoError(t, regexMatcher.Compile())

	rules := []configuration.Rule{
		{
			Action:               configuration.ActionRetry,
			OnTerminationMessage: regexMatcher,
		},
	}

	// Test matching message
	action := matcher.Match(&armadaevents.FailureInfo{
		ExitCode:           1,
		TerminationMessage: "Job failed with TRANSIENT_ERROR: connection timeout",
	}, rules)
	assert.Equal(t, configuration.ActionRetry, action)

	// Test another matching pattern
	action = matcher.Match(&armadaevents.FailureInfo{
		ExitCode:           1,
		TerminationMessage: "RETRY_ME: temporary failure",
	}, rules)
	assert.Equal(t, configuration.ActionRetry, action)

	// Test non-matching message
	action = matcher.Match(&armadaevents.FailureInfo{
		ExitCode:           1,
		TerminationMessage: "PERMANENT_ERROR: invalid input",
	}, rules)
	assert.Equal(t, configuration.ActionFail, action)

	// Test empty message doesn't match
	action = matcher.Match(&armadaevents.FailureInfo{
		ExitCode:           1,
		TerminationMessage: "",
	}, rules)
	assert.Equal(t, configuration.ActionFail, action)
}

func TestMatcher_TerminationMessageInvalidRegex(t *testing.T) {
	// Invalid regex should fail to compile
	regexMatcher := &configuration.RegexMatcher{
		Pattern: "[invalid", // Invalid regex
	}
	err := regexMatcher.Compile()
	assert.Error(t, err)

	// Engine creation should fail with invalid regex
	_, err = NewEngine(configuration.RetryPolicyConfig{
		Enabled:          true,
		GlobalMaxRetries: 20,
		Default: configuration.Policy{
			RetryLimit: 3,
			Rules: []configuration.Rule{
				{
					Action:               configuration.ActionRetry,
					OnTerminationMessage: regexMatcher,
				},
			},
		},
	})
	assert.Error(t, err)
}

func TestMatcher_ExitCodeZeroNeverMatches(t *testing.T) {
	// Exit code 0 is proto3 default value, treated as "not set" and never matches
	matcher := NewMatcher()
	rules := []configuration.Rule{{
		Action: configuration.ActionRetry,
		OnExitCodes: &configuration.ExitCodeMatcher{
			Operator: "In",
			Values:   []int32{0, 1, 2},
		},
	}}

	action := matcher.Match(&armadaevents.FailureInfo{ExitCode: 0}, rules)
	assert.Equal(t, configuration.ActionFail, action)
}

func TestEngine_NilFailureInfo(t *testing.T) {
	engine, err := NewEngine(configuration.RetryPolicyConfig{
		Enabled:          true,
		GlobalMaxRetries: 20,
		Default:          configuration.Policy{RetryLimit: 3},
	})
	require.NoError(t, err)

	result := engine.Evaluate("", nil, 0, 1)
	assert.Equal(t, configuration.ActionFail, result.Action)
	assert.False(t, result.ShouldRequeue)
	assert.False(t, result.IncrementFailureCount)
}

func TestMatcher_FirstMatchingRuleWins(t *testing.T) {
	// Verifies that rules are evaluated in order and first match wins
	matcher := NewMatcher()

	rules := []configuration.Rule{
		{
			// First rule: Fail on exit code 1
			Action: configuration.ActionFail,
			OnExitCodes: &configuration.ExitCodeMatcher{
				Operator: "In",
				Values:   []int32{1},
			},
		},
		{
			// Second rule: Retry all non-zero (would also match exit code 1)
			Action: configuration.ActionRetry,
			OnExitCodes: &configuration.ExitCodeMatcher{
				Operator: "NotIn",
				Values:   []int32{0},
			},
		},
	}

	// Exit code 1 matches both rules, but first rule should win (Fail)
	action := matcher.Match(&armadaevents.FailureInfo{ExitCode: 1}, rules)
	assert.Equal(t, configuration.ActionFail, action)

	// Exit code 2 only matches second rule (Retry)
	action = matcher.Match(&armadaevents.FailureInfo{ExitCode: 2}, rules)
	assert.Equal(t, configuration.ActionRetry, action)
}

func TestMatcher_MultipleMatchersOnlyFirstEvaluated(t *testing.T) {
	// When a rule has multiple matchers, only the first non-nil is evaluated.
	// Priority: OnConditions > OnExitCodes > OnTerminationMessage.
	matcher := NewMatcher()

	rules := []configuration.Rule{
		{
			Action: configuration.ActionRetry,
			// Both matchers set: OnConditions takes priority
			OnConditions: []configuration.FailureCondition{
				configuration.ConditionPreempted,
			},
			OnExitCodes: &configuration.ExitCodeMatcher{
				Operator: "In",
				Values:   []int32{1},
			},
		},
	}

	// Condition matches (Preempted) → Retry, even though exit code wouldn't match
	action := matcher.Match(&armadaevents.FailureInfo{
		Condition: armadaevents.FailureCondition_FAILURE_CONDITION_PREEMPTED,
		ExitCode:  99,
	}, rules)
	assert.Equal(t, configuration.ActionRetry, action)

	// Condition doesn't match (OOM) → Fail, even though exit code 1 would match OnExitCodes
	action = matcher.Match(&armadaevents.FailureInfo{
		Condition: armadaevents.FailureCondition_FAILURE_CONDITION_OOM_KILLED,
		ExitCode:  1,
	}, rules)
	assert.Equal(t, configuration.ActionFail, action)
}

func TestEngine_RetryAtLimitMinusOne(t *testing.T) {
	// Verifies that failureCount = retryLimit - 1 still allows retry
	engine, err := NewEngine(configuration.RetryPolicyConfig{
		Enabled:          true,
		GlobalMaxRetries: 20,
		Default: configuration.Policy{
			RetryLimit: 3,
			Rules: []configuration.Rule{
				{
					Action: configuration.ActionRetry,
					OnExitCodes: &configuration.ExitCodeMatcher{
						Operator: "NotIn",
						Values:   []int32{0},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// failureCount = 2, retryLimit = 3 → 2 >= 3 is false → should retry
	result := engine.Evaluate("", &armadaevents.FailureInfo{
		ExitCode: 1,
	}, 2, 3)

	assert.Equal(t, configuration.ActionRetry, result.Action)
	assert.True(t, result.ShouldRequeue)
	assert.True(t, result.IncrementFailureCount)
}

func TestEngine_GlobalMaxRetriesReachedBeforePolicyLimit(t *testing.T) {
	// Verifies global max retries is enforced even when policy limit not reached
	// globalMaxRetries: 3 means 3 retries allowed (4 total runs max)
	engine, err := NewEngine(configuration.RetryPolicyConfig{
		Enabled:          true,
		GlobalMaxRetries: 3, // Low global limit (3 retries = 4 runs max)
		Default: configuration.Policy{
			RetryLimit: 10, // High policy limit
			Rules: []configuration.Rule{
				{
					Action: configuration.ActionRetry,
					OnExitCodes: &configuration.ExitCodeMatcher{
						Operator: "NotIn",
						Values:   []int32{0},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// failureCount = 1 (under policy limit of 10)
	// totalRuns = 4, globalMaxRetries = 3
	// Check: 4 > 3 = true → global limit exceeded → Fail
	result := engine.Evaluate("", &armadaevents.FailureInfo{
		ExitCode: 1,
	}, 1, 4)

	assert.Equal(t, configuration.ActionFail, result.Action)
	assert.False(t, result.ShouldRequeue)
	assert.False(t, result.IncrementFailureCount)
}
