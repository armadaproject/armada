package retry

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/pkg/api"
)

func TestApiPolicyCache_Fetch(t *testing.T) {
	tests := map[string][]struct {
		policies []*api.RetryPolicy
		apiErr   error
		wantErr  bool
		served   []string
		missing  []string
	}{
		"fetch error then success": {
			{apiErr: errors.New("api unavailable"), wantErr: true, missing: []string{"test"}},
			{policies: []*api.RetryPolicy{
				{Name: "test", DefaultAction: api.RetryAction_RETRY_ACTION_RETRY},
			}, served: []string{"test"}},
		},
		"all policies invalid fails the refresh": {
			// Every policy fails to compile (unspecified default action).
			{policies: []*api.RetryPolicy{
				{Name: "bad-1", DefaultAction: api.RetryAction_RETRY_ACTION_UNSPECIFIED},
				{Name: "bad-2", DefaultAction: api.RetryAction_RETRY_ACTION_UNSPECIFIED},
			}, wantErr: true, missing: []string{"bad-1", "bad-2"}},
		},
		"mixed valid and invalid serves only the valid": {
			{policies: []*api.RetryPolicy{
				{Name: "good", DefaultAction: api.RetryAction_RETRY_ACTION_RETRY},
				{Name: "bad", DefaultAction: api.RetryAction_RETRY_ACTION_UNSPECIFIED},
			}, served: []string{"good"}, missing: []string{"bad"}},
		},
		"failed refresh keeps serving previous policies": {
			{policies: []*api.RetryPolicy{
				{Name: "keep", DefaultAction: api.RetryAction_RETRY_ACTION_RETRY},
			}, served: []string{"keep"}},
			{apiErr: errors.New("api unavailable"), wantErr: true, served: []string{"keep"}},
		},
	}

	for name, steps := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := armadacontext.Background()
			client := schedulermocks.NewMockRetryPolicyServiceClient(gomock.NewController(t))
			calls := make([]any, 0, len(steps))
			for _, s := range steps {
				if s.apiErr != nil {
					calls = append(calls, client.EXPECT().GetRetryPolicies(gomock.Any(), gomock.Any()).Return(nil, s.apiErr))
				} else {
					calls = append(calls, client.EXPECT().GetRetryPolicies(gomock.Any(), gomock.Any()).
						Return(&api.RetryPolicyList{RetryPolicies: s.policies}, nil))
				}
			}
			gomock.InOrder(calls...)
			cache := NewApiPolicyCache(client, time.Minute)

			for i, s := range steps {
				err := cache.fetch(ctx)
				if s.wantErr {
					require.Error(t, err, "step %d: fetch must fail", i)
				} else {
					require.NoError(t, err, "step %d: fetch must succeed", i)
				}
				for _, policyName := range s.served {
					policy, ok := cache.Get(policyName)
					require.True(t, ok, "step %d: policy %q must be served", i, policyName)
					assert.Equal(t, policyName, policy.Name)
				}
				for _, policyName := range s.missing {
					_, ok := cache.Get(policyName)
					assert.False(t, ok, "step %d: policy %q must not be served", i, policyName)
				}
			}
		})
	}
}
