package retry

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/errormatch"
	"github.com/armadaproject/armada/pkg/api"
)

func TestConvertPolicy_RoundTripAllMatchTypes(t *testing.T) {
	proto := &api.RetryPolicy{
		Name:          "policy-1",
		RetryLimit:    5,
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
		Rules: []*api.RetryRule{
			{
				Action:       api.RetryAction_RETRY_ACTION_RETRY,
				OnConditions: []string{errormatch.ConditionOOMKilled},
			},
			{
				Action: api.RetryAction_RETRY_ACTION_RETRY,
				OnExitCodes: &api.RetryExitCodeMatcher{
					Operator: api.ExitCodeOperator_EXIT_CODE_OPERATOR_IN,
					Values:   []int32{42, 137},
				},
			},
			{
				Action:                      api.RetryAction_RETRY_ACTION_FAIL,
				OnTerminationMessagePattern: "(?i)out of memory",
			},
			{
				Action:        api.RetryAction_RETRY_ACTION_RETRY,
				OnCategory:    "transient",
				OnSubcategory: "node-failure",
			},
		},
	}

	policy, err := ConvertPolicy(proto)
	require.NoError(t, err)
	require.NotNil(t, policy)

	assert.Equal(t, "policy-1", policy.Name)
	assert.Equal(t, uint32(5), policy.RetryLimit)
	assert.Equal(t, ActionFail, policy.DefaultAction)
	require.Len(t, policy.Rules, 4)

	// Conditions rule.
	assert.Equal(t, ActionRetry, policy.Rules[0].Action)
	assert.Equal(t, []string{errormatch.ConditionOOMKilled}, policy.Rules[0].OnConditions)

	// Exit-code rule.
	assert.Equal(t, ActionRetry, policy.Rules[1].Action)
	require.NotNil(t, policy.Rules[1].OnExitCodes)
	assert.Equal(t, errormatch.ExitCodeOperatorIn, policy.Rules[1].OnExitCodes.Operator)
	assert.Equal(t, []int32{42, 137}, policy.Rules[1].OnExitCodes.Values)

	// Termination-message rule.
	assert.Equal(t, ActionFail, policy.Rules[2].Action)
	require.NotNil(t, policy.Rules[2].OnTerminationMessage)
	assert.Equal(t, "(?i)out of memory", policy.Rules[2].OnTerminationMessage.Pattern)
	require.NotNil(t, policy.Rules[2].compiledTerminationMessage,
		"CompileRules must populate compiledTerminationMessage so the matcher does not fail closed at evaluation time")

	// Category rule.
	assert.Equal(t, ActionRetry, policy.Rules[3].Action)
	assert.Equal(t, "transient", policy.Rules[3].OnCategory)
	assert.Equal(t, "node-failure", policy.Rules[3].OnSubcategory)
}

func TestConvertPolicy_EmptyFieldsRemainEmpty(t *testing.T) {
	// Smoke test guarding against a common nil-vs-empty bug: proto3 omits
	// scalar zero values, so an unset on_subcategory deserialises as "" and
	// must remain "" on the engine Rule (not nil, not "<nil>").
	proto := &api.RetryPolicy{
		Name:          "empty-fields",
		RetryLimit:    1,
		DefaultAction: api.RetryAction_RETRY_ACTION_RETRY,
		Rules: []*api.RetryRule{
			{
				Action:     api.RetryAction_RETRY_ACTION_RETRY,
				OnCategory: "kubernetes",
				// OnSubcategory and other fields intentionally left zero.
			},
		},
	}

	policy, err := ConvertPolicy(proto)
	require.NoError(t, err)
	require.Len(t, policy.Rules, 1)
	rule := policy.Rules[0]
	assert.Equal(t, "kubernetes", rule.OnCategory)
	assert.Equal(t, "", rule.OnSubcategory)
	assert.Empty(t, rule.OnConditions)
	assert.Nil(t, rule.OnExitCodes)
	assert.Nil(t, rule.OnTerminationMessage)
}

func TestConvertPolicy_InvalidRegex(t *testing.T) {
	proto := &api.RetryPolicy{
		Name:          "bad-regex",
		RetryLimit:    1,
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
		Rules: []*api.RetryRule{
			{
				Action:                      api.RetryAction_RETRY_ACTION_RETRY,
				OnTerminationMessagePattern: "(unclosed",
			},
		},
	}

	policy, err := ConvertPolicy(proto)
	assert.Nil(t, policy)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad-regex",
		"error must mention the policy name so operators can locate the broken policy")
}

func TestConvertPolicy_UnknownAction(t *testing.T) {
	proto := &api.RetryPolicy{
		Name:          "unspecified",
		RetryLimit:    1,
		DefaultAction: api.RetryAction_RETRY_ACTION_UNSPECIFIED,
		Rules:         nil,
	}
	_, err := ConvertPolicy(proto)
	require.Error(t, err)
	// We refuse RETRY_ACTION_UNSPECIFIED rather than treating it as a default,
	// otherwise a truncated proto could silently retry every error.
	assert.True(t, strings.Contains(err.Error(), "unknown action"), err.Error())
}

func TestConvertPolicy_NilProto(t *testing.T) {
	policy, err := ConvertPolicy(nil)
	assert.Nil(t, policy)
	require.Error(t, err)
}

func TestConvertPolicy_InvalidExitCodeOperator(t *testing.T) {
	proto := &api.RetryPolicy{
		Name:          "bad-op",
		RetryLimit:    1,
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
		Rules: []*api.RetryRule{
			{
				Action: api.RetryAction_RETRY_ACTION_RETRY,
				OnExitCodes: &api.RetryExitCodeMatcher{
					Operator: api.ExitCodeOperator_EXIT_CODE_OPERATOR_UNSPECIFIED,
					Values:   []int32{1},
				},
			},
		},
	}
	_, err := ConvertPolicy(proto)
	require.Error(t, err)
}
