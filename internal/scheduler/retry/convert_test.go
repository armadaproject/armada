package retry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/pointer"
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
		"rule mutations are carried through": {
			proto: &api.RetryPolicy{
				Name:          "with-mutations",
				RetryLimit:    2,
				DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
				Rules: []*api.RetryRule{
					{
						Action:     api.RetryAction_RETRY_ACTION_RETRY,
						OnCategory: "oom",
						Mutate: &api.RetryMutation{
							Affinity:  &api.RetryAffinityMutation{AvoidSameNode: true},
							Resources: &api.RetryResourceMutation{Memory: &api.RetryResourceBump{Factor: 1.5}},
						},
					},
					{
						Action:        api.RetryAction_RETRY_ACTION_RETRY,
						OnCategory:    "oom",
						OnSubcategory: "small",
						Mutate: &api.RetryMutation{
							Resources: &api.RetryResourceMutation{Memory: &api.RetryResourceBump{Static: "512Mi"}},
						},
					},
					// A rule with no mutation and no subcategory: proto3 omits
					// both zero values, so this also checks Mutation and
					// OnSubcategory deserialise to their zero values, not nil.
					{Action: api.RetryAction_RETRY_ACTION_RETRY, OnCategory: "transient"},
				},
			},
			expected: &Policy{
				Name:          "with-mutations",
				RetryLimit:    2,
				DefaultAction: ActionFail,
				Rules: []Rule{
					{Action: ActionRetry, OnCategory: "oom", Mutation: Mutation{
						Affinity:  AffinityMutation{AvoidSameNode: true},
						Resources: ResourceMutation{Memory: ResourceBump{Factor: 1.5}},
					}},
					{Action: ActionRetry, OnCategory: "oom", OnSubcategory: "small", Mutation: Mutation{
						Resources: ResourceMutation{Memory: ResourceBump{Static: pointer.MustParseResource("512Mi")}},
					}},
					{Action: ActionRetry, OnCategory: "transient"},
				},
			},
		},
		"memory bump with invalid static quantity rejected": {
			proto:       policyWithMemoryBump(&api.RetryResourceBump{Static: "many"}),
			expectError: "invalid static quantity",
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

// policyWithMemoryBump builds a single-rule policy whose only interesting part
// is the memory bump under test.
func policyWithMemoryBump(bump *api.RetryResourceBump) *api.RetryPolicy {
	return &api.RetryPolicy{
		Name:          "memory-bump",
		DefaultAction: api.RetryAction_RETRY_ACTION_RETRY,
		Rules: []*api.RetryRule{{
			Action:     api.RetryAction_RETRY_ACTION_RETRY,
			OnCategory: "oom",
			Mutate: &api.RetryMutation{
				Resources: &api.RetryResourceMutation{Memory: bump},
			},
		}},
	}
}
