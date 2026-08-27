package retry

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/api"
)

// PolicyCache is the read-side interface the scheduler uses to look up
// compiled retry policies by name.
type PolicyCache interface {
	// Get returns the compiled policy for the given name. Returns (nil, false)
	// when the name is empty, the cache is empty, or the policy is unknown.
	// The returned policy is shared by all callers until the next refresh and
	// must not be modified.
	Get(name string) (*Policy, bool)
}

// ApiPolicyCache periodically fetches retry policies from the Armada API and
// keeps a compiled, in-memory copy. The cache fails open: if the API is
// unreachable the previously-cached policies are still served.
type ApiPolicyCache struct {
	updateFrequency time.Duration
	apiClient       api.RetryPolicyServiceClient
	policies        atomic.Pointer[map[string]*Policy]
}

// NewApiPolicyCache creates an ApiPolicyCache that refreshes every updateFrequency.
func NewApiPolicyCache(apiClient api.RetryPolicyServiceClient, updateFrequency time.Duration) *ApiPolicyCache {
	return &ApiPolicyCache{
		updateFrequency: updateFrequency,
		apiClient:       apiClient,
	}
}

// Run fetches once immediately, then refreshes the cache on the configured
// interval until ctx is cancelled. Errors are logged and do not stop the loop.
func (c *ApiPolicyCache) Run(ctx *armadacontext.Context) error {
	if err := c.fetch(ctx); err != nil {
		ctx.Warnf("error fetching retry policies: %v", err)
	}
	ticker := time.NewTicker(c.updateFrequency)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := c.fetch(ctx); err != nil {
				ctx.Warnf("error fetching retry policies: %v", err)
			}
		}
	}
}

func (c *ApiPolicyCache) Get(name string) (*Policy, bool) {
	if name == "" {
		return nil, false
	}
	m := c.policies.Load()
	if m == nil {
		return nil, false
	}
	policy, ok := (*m)[name]
	return policy, ok
}

func (c *ApiPolicyCache) fetch(ctx *armadacontext.Context) error {
	start := time.Now()
	resp, err := c.apiClient.GetRetryPolicies(ctx, &api.RetryPolicyListRequest{})
	if err != nil {
		return fmt.Errorf("get retry policies: %w", err)
	}
	compiled := make(map[string]*Policy, len(resp.RetryPolicies))
	for _, p := range resp.RetryPolicies {
		policy, err := ConvertPolicy(p)
		if err != nil {
			ctx.Warnf("skipping invalid retry policy %q: %v", p.GetName(), err)
			continue
		}
		compiled[p.Name] = policy
	}
	if len(resp.RetryPolicies) > 0 && len(compiled) == 0 {
		return fmt.Errorf("all %d retry policies failed to compile", len(resp.RetryPolicies))
	}
	c.policies.Store(&compiled)
	ctx.Infof("Refreshed %d retry policies in %s", len(compiled), time.Since(start))
	return nil
}

// NoopPolicyCache always reports the policy as missing. Used when the retry
// policy feature is disabled so the scheduler falls through to the legacy
// failure path.
type NoopPolicyCache struct{}

func (NoopPolicyCache) Get(string) (*Policy, bool) { return nil, false }
