package retry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/pkg/api"
)

func TestConvertPolicy(t *testing.T) {
	tests := map[string]struct {
		proto       *api.RetryPolicy
		expected    *Policy
		expectError string
	}{
		"all fields populated": {
			proto: &api.RetryPolicy{
				Name:          "policy-1",
				RetryLimit:    5,
				DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
				Rules: []*api.RetryRule{
					{
						Action:        api.RetryAction_RETRY_ACTION_RETRY,
						OnCategory:    "transient",
						OnSubcategory: "node-failure",
					},
				},
			},
			expected: &Policy{
				Name:          "policy-1",
				RetryLimit:    5,
				DefaultAction: ActionFail,
				Rules: []Rule{
					{Action: ActionRetry, OnCategory: "transient", OnSubcategory: "node-failure"},
				},
			},
		},
		"unspecified default action rejected": {
			proto: &api.RetryPolicy{
				Name:          "unspecified",
				RetryLimit:    1,
				DefaultAction: api.RetryAction_RETRY_ACTION_UNSPECIFIED,
			},
			expectError: "unknown action",
		},
		"unspecified rule action rejected": {
			proto: &api.RetryPolicy{
				Name:          "bad-rule",
				RetryLimit:    1,
				DefaultAction: api.RetryAction_RETRY_ACTION_RETRY,
				Rules: []*api.RetryRule{
					{Action: api.RetryAction_RETRY_ACTION_UNSPECIFIED, OnCategory: "gpu"},
				},
			},
			expectError: "unknown action",
		},
		"nil proto rejected": {
			proto:       nil,
			expectError: "nil",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			policy, err := ConvertPolicy(tc.proto)
			if tc.expectError != "" {
				assert.Nil(t, policy)
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectError)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, policy)
		})
	}
}
