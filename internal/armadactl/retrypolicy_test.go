package armadactl

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"github.com/armadaproject/armada/pkg/api"
)

func newTestApp() (*App, *bytes.Buffer) {
	out := &bytes.Buffer{}
	a := New()
	a.Out = out
	return a, out
}

func writeRetryPolicyFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.yaml")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

func fileWith(contents string) func(t *testing.T) string {
	return func(t *testing.T) string {
		return writeRetryPolicyFile(t, contents)
	}
}

const validPolicyFile = `apiVersion: armadaproject.io/v1beta1
kind: RetryPolicy
name: p1
retryLimit: 3
defaultAction: Retry
rules:
  - action: Fail
    onCategory: UserError
`

func TestGetRetryPolicy_RendersFriendlyActionStrings(t *testing.T) {
	a, out := newTestApp()
	a.Params.RetryPolicyAPI.Get = func(name string) (*api.RetryPolicy, error) {
		return &api.RetryPolicy{
			Name:          name,
			RetryLimit:    3,
			DefaultAction: api.RetryAction_RETRY_ACTION_RETRY,
			Rules: []*api.RetryRule{
				{Action: api.RetryAction_RETRY_ACTION_FAIL, OnCategory: "OutOfMemory"},
			},
		}, nil
	}

	require.NoError(t, a.GetRetryPolicy("p1"))

	got := out.String()
	assert.Contains(t, got, "kind: RetryPolicy")
	// Actions must render as friendly aliases, not raw enum integers.
	assert.Contains(t, got, "defaultAction: Retry")
	assert.Contains(t, got, "action: Fail")
	assert.NotContains(t, got, "action: 1")
	assert.NotContains(t, got, "defaultAction: 2")
}

func TestGetAllRetryPolicies_EmitsValidYaml(t *testing.T) {
	tests := map[string]struct {
		policies []*api.RetryPolicy
	}{
		"a populated store": {
			policies: []*api.RetryPolicy{
				{Name: "p1", DefaultAction: api.RetryAction_RETRY_ACTION_RETRY},
				{Name: "p2", DefaultAction: api.RetryAction_RETRY_ACTION_FAIL},
			},
		},
		// Its own case because "{}" after the header does not parse.
		"an empty store": {policies: nil},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			a, out := newTestApp()
			a.Params.RetryPolicyAPI.GetAll = func() ([]*api.RetryPolicy, error) {
				return tc.policies, nil
			}

			require.NoError(t, a.GetAllRetryPolicies())

			// Unmarshalling the whole output proves it is one mapping, not a
			// header followed by a bare sequence.
			var doc struct {
				APIVersion    string             `json:"apiVersion"`
				Kind          string             `json:"kind"`
				RetryPolicies []*api.RetryPolicy `json:"retryPolicies"`
			}
			require.NoError(t, yaml.Unmarshal(out.Bytes(), &doc), "get-list output must be valid YAML: %s", out.String())

			assert.Equal(t, "RetryPolicy", doc.Kind)
			require.Len(t, doc.RetryPolicies, len(tc.policies))
			for i, want := range tc.policies {
				assert.Equal(t, want.Name, doc.RetryPolicies[i].Name)
				assert.Equal(t, want.DefaultAction, doc.RetryPolicies[i].DefaultAction)
			}
		})
	}
}

// Each write command must hand the API what it was given and report what it did.
func TestRetryPolicyWriteCommands_CallAPIAndReport(t *testing.T) {
	tests := map[string]struct {
		stub    func(a *App, gotName *string)
		call    func(a *App) error
		wantOut string
	}{
		"create": {
			stub: func(a *App, gotName *string) {
				a.Params.RetryPolicyAPI.Create = func(policy *api.RetryPolicy) error {
					*gotName = policy.Name
					return nil
				}
			},
			call:    func(a *App) error { return a.CreateRetryPolicy(&api.RetryPolicy{Name: "p1"}) },
			wantOut: "Created retry policy p1",
		},
		"update": {
			stub: func(a *App, gotName *string) {
				a.Params.RetryPolicyAPI.Update = func(policy *api.RetryPolicy) error {
					*gotName = policy.Name
					return nil
				}
			},
			call:    func(a *App) error { return a.UpdateRetryPolicy(&api.RetryPolicy{Name: "p1"}) },
			wantOut: "Updated retry policy p1",
		},
		"delete": {
			stub: func(a *App, gotName *string) {
				a.Params.RetryPolicyAPI.Delete = func(name string) error {
					*gotName = name
					return nil
				}
			},
			call:    func(a *App) error { return a.DeleteRetryPolicy("p1") },
			wantOut: "Deleted retry policy p1",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			a, out := newTestApp()
			var gotName string
			tc.stub(a, &gotName)

			require.NoError(t, tc.call(a))
			assert.Equal(t, "p1", gotName)
			assert.Contains(t, out.String(), tc.wantOut)
		})
	}
}

func TestRetryPolicyFromFile_Parses(t *testing.T) {
	tests := map[string]struct {
		path      func(t *testing.T) string
		wantLimit uint32
	}{
		"a handwritten document": {
			path:      fileWith(validPolicyFile),
			wantLimit: 3,
		},
		// get output must be usable as create input.
		"the output of get": {
			path: func(t *testing.T) string {
				a, out := newTestApp()
				a.Params.RetryPolicyAPI.Get = func(name string) (*api.RetryPolicy, error) {
					return &api.RetryPolicy{
						Name:          name,
						RetryLimit:    2,
						DefaultAction: api.RetryAction_RETRY_ACTION_RETRY,
						Rules:         []*api.RetryRule{{Action: api.RetryAction_RETRY_ACTION_FAIL, OnCategory: "UserError"}},
					}, nil
				}
				require.NoError(t, a.GetRetryPolicy("p1"))
				return writeRetryPolicyFile(t, out.String())
			},
			wantLimit: 2,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			policy, err := retryPolicyFromFile(tc.path(t))
			require.NoError(t, err)

			assert.Equal(t, "p1", policy.Name)
			assert.Equal(t, tc.wantLimit, policy.RetryLimit)
			assert.Equal(t, api.RetryAction_RETRY_ACTION_RETRY, policy.DefaultAction)
			require.Len(t, policy.Rules, 1)
			assert.Equal(t, "UserError", policy.Rules[0].OnCategory)
			assert.Equal(t, api.RetryAction_RETRY_ACTION_FAIL, policy.Rules[0].Action)
		})
	}
}

func TestRetryPolicyFromFile_Rejects(t *testing.T) {
	tests := map[string]struct {
		path    func(t *testing.T) string
		wantErr string
	}{
		"a mistyped field": {
			path: fileWith(`apiVersion: armadaproject.io/v1beta1
kind: RetryPolicy
name: p1
retry_limit: 5
defaultAction: Retry
`),
			wantErr: "unknown field",
		},
		"a file describing another resource": {
			path: fileWith(`apiVersion: armadaproject.io/v1beta1
kind: Queue
name: q1
`),
			wantErr: "kind must be",
		},
		"a bare document with no envelope": {
			path:    fileWith("name: p1\nretryLimit: 3\ndefaultAction: Retry\n"),
			wantErr: "apiVersion must be",
		},
		"an unknown kind": {
			path:    fileWith("apiVersion: armadaproject.io/v1beta1\nkind: Nonsense\nname: p1\n"),
			wantErr: "invalid kind",
		},
		"an unknown apiVersion": {
			path:    fileWith("apiVersion: armadaproject.io/v2\nkind: RetryPolicy\nname: p1\n"),
			wantErr: "invalid version",
		},
		"malformed yaml": {
			path:    fileWith("apiVersion: [unclosed\n"),
			wantErr: "converting YAML to JSON",
		},
		"a missing file": {
			path:    func(t *testing.T) string { return filepath.Join(t.TempDir(), "absent.yaml") },
			wantErr: "no such file",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := retryPolicyFromFile(tc.path(t))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
