package armadactl

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/retrypolicy"
)

func newTestApp(out *bytes.Buffer) *App {
	a := New()
	a.Out = out
	return a
}

func TestGetRetryPolicy_RendersFriendlyActionStrings(t *testing.T) {
	out := &bytes.Buffer{}
	a := newTestApp(out)
	a.Params.RetryPolicyAPI.Get = retrypolicy.GetAPI(func(name string) (*api.RetryPolicy, error) {
		return &api.RetryPolicy{
			Name:          name,
			RetryLimit:    3,
			DefaultAction: api.RetryAction_RETRY_ACTION_RETRY,
			Rules: []*api.RetryRule{
				{Action: api.RetryAction_RETRY_ACTION_FAIL, OnCategory: "OutOfMemory"},
			},
		}, nil
	})

	require.NoError(t, a.GetRetryPolicy("p1"))

	got := out.String()
	assert.Contains(t, got, "kind: RetryPolicy")
	// Actions must render as friendly aliases, not raw enum integers.
	assert.Contains(t, got, "defaultAction: Retry")
	assert.Contains(t, got, "action: Fail")
	assert.NotContains(t, got, "action: 1")
	assert.NotContains(t, got, "defaultAction: 2")
}

func TestGetAllRetryPolicies_EmitsValidYamlList(t *testing.T) {
	out := &bytes.Buffer{}
	a := newTestApp(out)
	a.Params.RetryPolicyAPI.GetAll = retrypolicy.GetAllAPI(func() ([]*api.RetryPolicy, error) {
		return []*api.RetryPolicy{
			{Name: "p1", DefaultAction: api.RetryAction_RETRY_ACTION_RETRY},
			{Name: "p2", DefaultAction: api.RetryAction_RETRY_ACTION_FAIL},
		}, nil
	})

	require.NoError(t, a.GetAllRetryPolicies())

	// The whole document must parse as one mapping: header keys plus a
	// retryPolicies list. A mapping header followed by a bare sequence is not
	// valid YAML.
	var doc struct {
		APIVersion    string             `json:"apiVersion"`
		Kind          string             `json:"kind"`
		RetryPolicies []*api.RetryPolicy `json:"retryPolicies"`
	}
	require.NoError(t, yaml.Unmarshal(out.Bytes(), &doc), "get-list output must be valid YAML: %s", out.String())

	assert.Equal(t, "RetryPolicy", doc.Kind)
	require.Len(t, doc.RetryPolicies, 2)
	assert.Equal(t, "p1", doc.RetryPolicies[0].Name)
	assert.Equal(t, api.RetryAction_RETRY_ACTION_RETRY, doc.RetryPolicies[0].DefaultAction)
	assert.Equal(t, "p2", doc.RetryPolicies[1].Name)
	assert.Equal(t, api.RetryAction_RETRY_ACTION_FAIL, doc.RetryPolicies[1].DefaultAction)
}

func TestGetAllRetryPolicies_EmptyListEmitsValidYaml(t *testing.T) {
	out := &bytes.Buffer{}
	a := newTestApp(out)
	a.Params.RetryPolicyAPI.GetAll = retrypolicy.GetAllAPI(func() ([]*api.RetryPolicy, error) {
		return nil, nil
	})

	require.NoError(t, a.GetAllRetryPolicies())

	// An empty store must still emit valid YAML: a header glued to a bare "{}"
	// does not parse.
	var doc struct {
		Kind          string             `json:"kind"`
		RetryPolicies []*api.RetryPolicy `json:"retryPolicies"`
	}
	require.NoError(t, yaml.Unmarshal(out.Bytes(), &doc), "empty get-list output must be valid YAML: %s", out.String())
	assert.Equal(t, "RetryPolicy", doc.Kind)
	assert.Empty(t, doc.RetryPolicies)
}

func TestCreateRetryPolicy_CallsAPIAndReports(t *testing.T) {
	out := &bytes.Buffer{}
	a := newTestApp(out)
	var got *api.RetryPolicy
	a.Params.RetryPolicyAPI.Create = retrypolicy.CreateAPI(func(policy *api.RetryPolicy) error {
		got = policy
		return nil
	})

	require.NoError(t, a.CreateRetryPolicy(&api.RetryPolicy{Name: "p1"}))
	require.NotNil(t, got)
	assert.Equal(t, "p1", got.Name)
	assert.Contains(t, out.String(), "Created retry policy p1")
}

func TestUpdateRetryPolicy_CallsAPIAndReports(t *testing.T) {
	out := &bytes.Buffer{}
	a := newTestApp(out)
	var got *api.RetryPolicy
	a.Params.RetryPolicyAPI.Update = retrypolicy.UpdateAPI(func(policy *api.RetryPolicy) error {
		got = policy
		return nil
	})

	require.NoError(t, a.UpdateRetryPolicy(&api.RetryPolicy{Name: "p1"}))
	require.NotNil(t, got)
	assert.Equal(t, "p1", got.Name)
	assert.Contains(t, out.String(), "Updated retry policy p1")
}

func TestDeleteRetryPolicy_CallsAPIAndReports(t *testing.T) {
	out := &bytes.Buffer{}
	a := newTestApp(out)
	var deleted string
	a.Params.RetryPolicyAPI.Delete = retrypolicy.DeleteAPI(func(name string) error {
		deleted = name
		return nil
	})

	require.NoError(t, a.DeleteRetryPolicy("p1"))
	assert.Equal(t, "p1", deleted)
	assert.Contains(t, out.String(), "Deleted retry policy p1")
}
