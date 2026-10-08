package retry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

func makeAppError(exitCode int32, message string) *armadaevents.Error {
	return &armadaevents.Error{
		Reason: &armadaevents.Error_PodError{
			PodError: &armadaevents.PodError{
				KubernetesReason: armadaevents.KubernetesReason_AppError,
				ContainerErrors:  []*armadaevents.ContainerError{{ExitCode: exitCode, Message: message}},
			},
		},
	}
}

func TestEngine_Evaluate(t *testing.T) {
	tests := map[string]struct {
		globalMax uint
		policy    *Policy
		runError  *armadaevents.Error
		counts    Counts
		expected  Result
	}{
		"category match (any subcategory)": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    10,
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{Action: ActionFail, OnCategory: "gpu"},
				},
			},
			runError: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{KubernetesReason: armadaevents.KubernetesReason_AppError},
				},
				FailureCategory:    "gpu",
				FailureSubcategory: "transient",
			},
			counts:   Counts{Failures: 1},
			expected: Result{ShouldRetry: false, Reason: "matched rule: Fail", Decision: DecisionFailRule},
		},
		"category match with subcategory match": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    10,
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{Action: ActionFail, OnCategory: "gpu", OnSubcategory: "transient"},
				},
			},
			runError: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{KubernetesReason: armadaevents.KubernetesReason_AppError},
				},
				FailureCategory:    "gpu",
				FailureSubcategory: "transient",
			},
			counts:   Counts{Failures: 1},
			expected: Result{ShouldRetry: false, Reason: "matched rule: Fail", Decision: DecisionFailRule},
		},
		"matched Retry rule carries its mutation": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    10,
				DefaultAction: ActionFail,
				Rules: []Rule{
					{Action: ActionRetry, OnCategory: "oom", Mutation: Mutation{Affinity: AffinityMutation{AvoidSameNode: true}}},
				},
			},
			runError: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{KubernetesReason: armadaevents.KubernetesReason_AppError},
				},
				FailureCategory: "oom",
			},
			counts:   Counts{Failures: 1},
			expected: Result{ShouldRetry: true, Reason: "matched rule: Retry", Decision: DecisionRetry, Mutation: Mutation{Affinity: AffinityMutation{AvoidSameNode: true}}},
		},
		"default-action retry carries no mutation": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    10,
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{Action: ActionFail, OnCategory: "user-error"},
				},
			},
			runError: makeAppError(1, "crash"), // no category, no rule matches
			counts:   Counts{Failures: 1},
			expected: Result{ShouldRetry: true, Reason: "no rule matched, using default action", Decision: DecisionRetry},
		},
		"matched Retry rule overrides Fail default": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    10,
				DefaultAction: ActionFail,
				Rules: []Rule{
					{Action: ActionRetry, OnCategory: "transient"},
				},
			},
			runError: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{KubernetesReason: armadaevents.KubernetesReason_AppError},
				},
				FailureCategory: "transient",
			},
			counts:   Counts{Failures: 1},
			expected: Result{ShouldRetry: true, Reason: "matched rule: Retry", Decision: DecisionRetry},
		},
		"category match but subcategory mismatch": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    10,
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{Action: ActionFail, OnCategory: "gpu", OnSubcategory: "permanent"},
				},
			},
			runError: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{KubernetesReason: armadaevents.KubernetesReason_AppError},
				},
				FailureCategory:    "gpu",
				FailureSubcategory: "transient",
			},
			counts:   Counts{Failures: 1},
			expected: Result{ShouldRetry: true, Reason: "no rule matched, using default action", Decision: DecisionRetry},
		},
		"category mismatch": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    10,
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{Action: ActionFail, OnCategory: "network"},
				},
			},
			runError: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{KubernetesReason: armadaevents.KubernetesReason_AppError},
				},
				FailureCategory:    "gpu",
				FailureSubcategory: "transient",
			},
			counts:   Counts{Failures: 1},
			expected: Result{ShouldRetry: true, Reason: "no rule matched, using default action", Decision: DecisionRetry},
		},
		"global cap exceeded": {
			globalMax: 5,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    10,
				DefaultAction: ActionRetry,
			},
			runError: makeAppError(1, "crash"),
			// This job has failed six times, so five retries have already been
			// granted. That reaches the global cap of five.
			counts:   Counts{Failures: 6},
			expected: Result{ShouldRetry: false, Reason: "global max retries exceeded (5/5)", Decision: DecisionFailGlobalLimit},
		},
		"retry limit exceeded": {
			globalMax: 100,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    3,
				DefaultAction: ActionRetry,
			},
			runError: makeAppError(1, "crash"),
			// This job has failed four times, so three retries have already been
			// granted. That reaches the per-policy limit of three.
			counts:   Counts{Failures: 4},
			expected: Result{ShouldRetry: false, Reason: "policy retry limit exceeded (3/3)", Decision: DecisionFailPolicyLimit},
		},
		"retry limit 0 never retries": {
			globalMax: 100,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    0,
				DefaultAction: ActionRetry,
			},
			runError: makeAppError(1, "crash"),
			counts:   Counts{Failures: 1},
			expected: Result{ShouldRetry: false, Reason: "policy retry limit exceeded (0/0)", Decision: DecisionFailPolicyLimit},
		},
		"nil error returns fail": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    10,
				DefaultAction: ActionRetry,
			},
			runError: nil,
			counts:   Counts{Failures: 1},
			expected: Result{ShouldRetry: false, Reason: "no error information available", Decision: DecisionNoError},
		},
		"empty rules returns DefaultAction": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    10,
				DefaultAction: ActionFail,
				Rules:         []Rule{},
			},
			runError: makeAppError(1, "crash"),
			counts:   Counts{Failures: 1},
			expected: Result{ShouldRetry: false, Reason: "no rule matched, using default action", Decision: DecisionFailDefault},
		},
		"globalMaxRetries 0 disables retries": {
			globalMax: 0,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    10,
				DefaultAction: ActionRetry,
			},
			runError: makeAppError(1, "crash"),
			counts:   Counts{Failures: 1},
			expected: Result{ShouldRetry: false, Reason: "global max retries is 0, retries disabled", Decision: DecisionFailGlobalLimit},
		},
		"first matching rule wins when two rules match": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    10,
				DefaultAction: ActionRetry,
				Rules: []Rule{
					{Action: ActionFail, OnCategory: "gpu"},
					{Action: ActionRetry, OnCategory: "gpu", OnSubcategory: "transient"},
				},
			},
			runError: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{KubernetesReason: armadaevents.KubernetesReason_AppError},
				},
				FailureCategory:    "gpu",
				FailureSubcategory: "transient",
			},
			counts:   Counts{Failures: 1},
			expected: Result{ShouldRetry: false, Reason: "matched rule: Fail", Decision: DecisionFailRule},
		},
		"uncategorized error with rules present falls to default action": {
			globalMax: 10,
			policy: &Policy{
				Name:          "test",
				RetryLimit:    10,
				DefaultAction: ActionFail,
				Rules: []Rule{
					{Action: ActionRetry, OnCategory: "gpu"},
				},
			},
			runError: makeAppError(1, "crash"), // no FailureCategory set
			counts:   Counts{Failures: 1},
			expected: Result{ShouldRetry: false, Reason: "no rule matched, using default action", Decision: DecisionFailDefault},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			engine := NewEngine(tc.globalMax)
			result, decidingPolicy := engine.Evaluate([]*Policy{tc.policy}, tc.runError, tc.counts)
			assert.Equal(t, tc.expected, result)
			assert.Same(t, tc.policy, decidingPolicy)
		})
	}
}

func TestEngine_Evaluate_PolicyList(t *testing.T) {
	high := &Policy{
		Name:          "high",
		RetryLimit:    1,
		DefaultAction: ActionFail,
		Rules:         []Rule{{Action: ActionRetry, OnCategory: "gpu"}},
	}
	low := &Policy{
		Name:          "low",
		RetryLimit:    3,
		DefaultAction: ActionRetry,
		Rules: []Rule{
			{Action: ActionFail, OnCategory: "gpu"},
			{Action: ActionRetry, OnCategory: "oom"},
		},
	}
	categorized := func(category string) *armadaevents.Error {
		err := makeAppError(1, "crash")
		err.FailureCategory = category
		return err
	}
	tests := map[string]struct {
		category       string
		failures       uint32
		expected       Result
		wantDecidingBy *Policy
	}{
		"the first matching rule decides, also when a lower policy has a rule for the category": {
			category:       "gpu",
			failures:       1,
			expected:       Result{ShouldRetry: true, Reason: "matched rule: Retry", Decision: DecisionRetry},
			wantDecidingBy: high,
		},
		"a rule of a lower policy decides with the retry limit of its own policy": {
			category:       "oom",
			failures:       3,
			expected:       Result{ShouldRetry: true, Reason: "matched rule: Retry", Decision: DecisionRetry},
			wantDecidingBy: low,
		},
		"without a matching rule the default action of the first policy decides": {
			category:       "uncategorized",
			failures:       1,
			expected:       Result{ShouldRetry: false, Reason: "no rule matched, using default action", Decision: DecisionFailDefault},
			wantDecidingBy: high,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, decidingPolicy := NewEngine(10).Evaluate([]*Policy{high, low}, categorized(tc.category), Counts{Failures: tc.failures})
			assert.Equal(t, tc.expected, result)
			assert.Same(t, tc.wantDecidingBy, decidingPolicy)
		})
	}
}

func TestValidatePolicy(t *testing.T) {
	tests := map[string]struct {
		policy      Policy
		expectError string
	}{
		"valid policy with Retry default": {
			policy: Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
			},
		},
		"valid policy with OnCategory rule": {
			policy: Policy{
				Name:          "test",
				DefaultAction: ActionFail,
				Rules: []Rule{
					{Action: ActionRetry, OnCategory: "transient"},
				},
			},
		},
		"empty name rejected": {
			policy:      Policy{Name: "", DefaultAction: ActionRetry},
			expectError: "policy name must not be empty",
		},
		"empty DefaultAction rejected": {
			policy:      Policy{Name: "test", DefaultAction: ""},
			expectError: "DefaultAction must be",
		},
		"unknown DefaultAction rejected": {
			policy:      Policy{Name: "test", DefaultAction: "Skip"},
			expectError: "DefaultAction must be",
		},
		"rule without OnCategory rejected": {
			policy: Policy{
				Name:          "test",
				DefaultAction: ActionRetry,
				Rules:         []Rule{{Action: ActionRetry, OnCategory: ""}},
			},
			expectError: "rule 0: OnCategory must be set",
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
