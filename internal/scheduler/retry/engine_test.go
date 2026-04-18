package retry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/errormatch"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func makeOOMError() *armadaevents.Error {
	return &armadaevents.Error{
		Reason: &armadaevents.Error_PodError{
			PodError: &armadaevents.PodError{
				KubernetesReason: armadaevents.KubernetesReason_OOM,
				ContainerErrors:  []*armadaevents.ContainerError{{ExitCode: 137, Message: "OOMKilled"}},
			},
		},
		FailureInfo: &armadaevents.FailureInfo{
			ExitCode:           137,
			TerminationMessage: "OOMKilled",
			Categories:         []string{"infrastructure"},
		},
	}
}

func makeAppError(exitCode int32, message string) *armadaevents.Error {
	return &armadaevents.Error{
		Reason: &armadaevents.Error_PodError{
			PodError: &armadaevents.PodError{
				KubernetesReason: armadaevents.KubernetesReason_AppError,
				ContainerErrors:  []*armadaevents.ContainerError{{ExitCode: exitCode, Message: message}},
			},
		},
		FailureInfo: &armadaevents.FailureInfo{
			ExitCode:           exitCode,
			TerminationMessage: message,
		},
	}
}

func compilePolicy(t *testing.T, p *Policy) *Policy {
	t.Helper()
	require.NoError(t, CompileRules(p.Rules))
	return p
}

func TestEngine_Evaluate(t *testing.T) {
	tests := map[string]struct {
		globalMax    uint
		policy       *Policy
		runError     *armadaevents.Error
		failureCount uint32
		totalRuns    uint
		expected     Result
	}{
		"condition match OOMKilled, action Fail": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{Action: ActionFail, OnConditions: []string{errormatch.ConditionOOMKilled}},
				},
			},
			runError:     makeOOMError(),
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: false, Reason: "matched rule: Fail"},
		},
		"condition match Evicted, action Retry": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionFail,
				Rules: []Rule{
					{Action: ActionRetry, OnConditions: []string{errormatch.ConditionEvicted}},
				},
			},
			runError: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{KubernetesReason: armadaevents.KubernetesReason_Evicted},
				},
			},
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: true, Reason: "matched rule: Retry"},
		},
		"condition match DeadlineExceeded": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{Action: ActionFail, OnConditions: []string{errormatch.ConditionDeadlineExceeded}},
				},
			},
			runError: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{KubernetesReason: armadaevents.KubernetesReason_DeadlineExceeded},
				},
			},
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: false, Reason: "matched rule: Fail"},
		},
		"condition match Preempted": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionFail,
				Rules: []Rule{
					{Action: ActionRetry, OnConditions: []string{errormatch.ConditionPreempted}},
				},
			},
			runError: &armadaevents.Error{
				Reason: &armadaevents.Error_JobRunPreemptedError{
					JobRunPreemptedError: &armadaevents.JobRunPreemptedError{},
				},
			},
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: true, Reason: "matched rule: Retry"},
		},
		"condition match LeaseReturned": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionFail,
				Rules: []Rule{
					{Action: ActionRetry, OnConditions: []string{errormatch.ConditionLeaseReturned}},
				},
			},
			runError: &armadaevents.Error{
				Reason: &armadaevents.Error_PodLeaseReturned{
					PodLeaseReturned: &armadaevents.PodLeaseReturned{},
				},
			},
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: true, Reason: "matched rule: Retry"},
		},
		"condition match AppError": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{Action: ActionFail, OnConditions: []string{errormatch.ConditionAppError}},
				},
			},
			runError:     makeAppError(1, "crash"),
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: false, Reason: "matched rule: Fail"},
		},
		"exit code In match": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{
						Action:      ActionFail,
						OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{42, 43}},
					},
				},
			},
			runError:     makeAppError(42, ""),
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: false, Reason: "matched rule: Fail"},
		},
		"exit code NotIn match": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionFail,
				Rules: []Rule{
					{
						Action:      ActionRetry,
						OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorNotIn, Values: []int32{42}},
					},
				},
			},
			runError:     makeAppError(1, ""),
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: true, Reason: "matched rule: Retry"},
		},
		"termination message regex match": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{
						Action:               ActionFail,
						OnTerminationMessage: &errormatch.RegexMatcher{Pattern: "(?i)cuda.*error"},
					},
				},
			},
			runError:     makeAppError(1, "CUDA memory error on device 0"),
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: false, Reason: "matched rule: Fail"},
		},
		"termination message regex no match": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{
						Action:               ActionFail,
						OnTerminationMessage: &errormatch.RegexMatcher{Pattern: "(?i)cuda.*error"},
					},
				},
			},
			runError:     makeAppError(1, "segfault"),
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: true, Reason: "no rule matched, using default action"},
		},
		"category match with overlap": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{Action: ActionFail, OnCategories: []string{"gpu", "network"}},
				},
			},
			runError: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{KubernetesReason: armadaevents.KubernetesReason_AppError},
				},
				FailureInfo: &armadaevents.FailureInfo{Categories: []string{"gpu", "transient"}},
			},
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: false, Reason: "matched rule: Fail"},
		},
		"category match without overlap": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{Action: ActionFail, OnCategories: []string{"network"}},
				},
			},
			runError: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{KubernetesReason: armadaevents.KubernetesReason_AppError},
				},
				FailureInfo: &armadaevents.FailureInfo{Categories: []string{"gpu", "transient"}},
			},
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: true, Reason: "no rule matched, using default action"},
		},
		"first match wins": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionFail,
				Rules: []Rule{
					{Action: ActionRetry, OnConditions: []string{errormatch.ConditionAppError}},
					{Action: ActionFail, OnConditions: []string{errormatch.ConditionAppError}},
				},
			},
			runError:     makeAppError(1, "crash"),
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: true, Reason: "matched rule: Retry"},
		},
		"no match returns DefaultAction Fail": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionFail,
				Rules: []Rule{
					{Action: ActionRetry, OnConditions: []string{errormatch.ConditionOOMKilled}},
				},
			},
			runError:     makeAppError(1, ""),
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: false, Reason: "no rule matched, using default action"},
		},
		"no match returns DefaultAction Retry": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{Action: ActionFail, OnConditions: []string{errormatch.ConditionOOMKilled}},
				},
			},
			runError:     makeAppError(1, ""),
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: true, Reason: "no rule matched, using default action"},
		},
		"global cap exceeded": {
			globalMax: 5,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
			},
			runError:     makeAppError(1, "crash"),
			failureCount: 0,
			totalRuns:    5,
			expected:     Result{ShouldRetry: false, Reason: "global max retries exceeded (5/5)"},
		},
		"retry limit exceeded": {
			globalMax: 100,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    3,
				DefaultAction: ActionRetry,
			},
			runError:     makeAppError(1, "crash"),
			failureCount: 3,
			totalRuns:    3,
			expected:     Result{ShouldRetry: false, Reason: "policy retry limit exceeded (3/3)"},
		},
		"retry limit 0 means unlimited within global cap": {
			globalMax: 100,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    0,
				DefaultAction: ActionRetry,
			},
			runError:     makeAppError(1, "crash"),
			failureCount: 50,
			totalRuns:    50,
			expected:     Result{ShouldRetry: true, Reason: "no rule matched, using default action"},
		},
		"nil error returns fail": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
			},
			runError:     nil,
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: false, Reason: "no error information available"},
		},
		"nil FailureInfo falls back to ContainerError fields": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{
						Action:      ActionFail,
						OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{42}},
					},
				},
			},
			runError: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{
						KubernetesReason: armadaevents.KubernetesReason_AppError,
						ContainerErrors:  []*armadaevents.ContainerError{{ExitCode: 42, Message: "custom exit"}},
					},
				},
			},
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: false, Reason: "matched rule: Fail"},
		},
		"AND logic, all fields must match": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{
						Action:       ActionFail,
						OnConditions: []string{errormatch.ConditionAppError},
						OnExitCodes:  &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{42}},
					},
				},
			},
			// Condition matches but exit code does not
			runError:     makeAppError(1, ""),
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: true, Reason: "no rule matched, using default action"},
		},
		"AND logic, both fields match": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{
						Action:       ActionFail,
						OnConditions: []string{errormatch.ConditionAppError},
						OnExitCodes:  &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{42}},
					},
				},
			},
			runError:     makeAppError(42, ""),
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: false, Reason: "matched rule: Fail"},
		},
		"empty rules returns DefaultAction": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionFail,
				Rules:         []Rule{},
			},
			runError:     makeAppError(1, "crash"),
			failureCount: 0,
			totalRuns:    1,
			expected:     Result{ShouldRetry: false, Reason: "no rule matched, using default action"},
		},
		"globalMaxRetries 0 means unlimited": {
			globalMax: 0,
			policy: &Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
			},
			runError:     makeAppError(1, "crash"),
			failureCount: 100,
			totalRuns:    100,
			expected:     Result{ShouldRetry: true, Reason: "no rule matched, using default action"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tc.policy = compilePolicy(t, tc.policy)
			engine := NewEngine(tc.globalMax)
			result := engine.Evaluate(tc.policy, tc.runError, tc.failureCount, tc.totalRuns)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestCompileRules_InvalidRegex(t *testing.T) {
	rules := []Rule{
		{
			Action:               ActionFail,
			OnTerminationMessage: &errormatch.RegexMatcher{Pattern: "[invalid"},
		},
	}
	err := CompileRules(rules)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to compile termination message pattern")
}

func TestValidatePolicy(t *testing.T) {
	tests := map[string]struct {
		policy      Policy
		expectError string
	}{
		"valid policy with Fail default": {
			policy: Policy{
				Name:          "test",
				DefaultAction: ActionFail,
				Rules: []Rule{
					{Action: ActionRetry, OnConditions: []string{"OOMKilled"}},
				},
			},
		},
		"valid policy with Retry default": {
			policy: Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
			},
		},
		"empty DefaultAction rejected": {
			policy:      Policy{Name: "test", DefaultAction: ""},
			expectError: "DefaultAction must be",
		},
		"unknown DefaultAction rejected": {
			policy:      Policy{Name: "test", DefaultAction: "Skip"},
			expectError: "DefaultAction must be",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidatePolicy(tc.policy)
			if tc.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCompileRules_Validation(t *testing.T) {
	tests := map[string]struct {
		rules       []Rule
		expectError string
	}{
		"empty termination message pattern": {
			rules: []Rule{
				{
					Action:               ActionFail,
					OnTerminationMessage: &errormatch.RegexMatcher{Pattern: ""},
				},
			},
			expectError: "rule 0: OnTerminationMessage pattern must not be empty",
		},
		"empty rule with no match fields": {
			rules: []Rule{
				{Action: ActionFail},
			},
			expectError: "rule 0: must have at least one match field",
		},
		"invalid exit code operator": {
			rules: []Rule{
				{
					Action:      ActionFail,
					OnExitCodes: &errormatch.ExitCodeMatcher{Operator: "BadOp", Values: []int32{1}},
				},
			},
			expectError: "rule 0: OnExitCodes operator must be",
		},
		"empty exit code values": {
			rules: []Rule{
				{
					Action:      ActionFail,
					OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{}},
				},
			},
			expectError: "rule 0: OnExitCodes values must not be empty",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := CompileRules(tc.rules)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectError)
		})
	}
}
