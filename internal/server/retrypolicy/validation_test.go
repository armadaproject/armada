package retrypolicy

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/pkg/api"
)

func TestValidatePolicy(t *testing.T) {
	tests := map[string]struct {
		policy  *api.RetryPolicy
		wantErr string // empty means the policy is expected to validate
	}{
		"nil policy": {
			policy:  nil,
			wantErr: "must not be nil",
		},
		"empty name": {
			policy:  &api.RetryPolicy{DefaultAction: api.RetryAction_RETRY_ACTION_FAIL},
			wantErr: "name must not be empty",
		},
		"name with uppercase characters": {
			policy: &api.RetryPolicy{
				Name:          "MyPolicy",
				DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
			},
			wantErr: "is invalid",
		},
		"name with leading dash": {
			policy: &api.RetryPolicy{
				Name:          "-policy",
				DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
			},
			wantErr: "is invalid",
		},
		"name with trailing dash": {
			policy: &api.RetryPolicy{
				Name:          "policy-",
				DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
			},
			wantErr: "is invalid",
		},
		"name too long": {
			policy: &api.RetryPolicy{
				Name:          strings.Repeat("a", maxPolicyNameLength+1),
				DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
			},
			wantErr: "at most 63 characters",
		},
		"nil rule": {
			policy: &api.RetryPolicy{
				Name:  "p1",
				Rules: []*api.RetryRule{nil},
			},
			wantErr: "rule must not be nil",
		},
		"rule with unspecified action": {
			policy: &api.RetryPolicy{
				Name: "p1",
				Rules: []*api.RetryRule{
					{},
				},
			},
			wantErr: "action must be Fail or Retry",
		},
		"rule with no matchers": {
			policy: &api.RetryPolicy{
				Name: "p1",
				Rules: []*api.RetryRule{
					{Action: api.RetryAction_RETRY_ACTION_RETRY},
				},
			},
			wantErr: "on_category must be set",
		},
		"rule with only subcategory": {
			policy: &api.RetryPolicy{
				Name: "p1",
				Rules: []*api.RetryRule{
					{
						Action:        api.RetryAction_RETRY_ACTION_RETRY,
						OnSubcategory: "oom",
					},
				},
			},
			wantErr: "on_category must be set",
		},
		"valid policy with default action only": {
			policy: &api.RetryPolicy{
				Name:          "p1",
				DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
			},
		},
		"valid policy with on_category rule": {
			policy: &api.RetryPolicy{
				Name:          "p1",
				DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
				Rules: []*api.RetryRule{
					{
						Action:     api.RetryAction_RETRY_ACTION_RETRY,
						OnCategory: "transient",
					},
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidatePolicy(tc.policy)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
