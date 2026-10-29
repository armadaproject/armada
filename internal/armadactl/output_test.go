package armadactl

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

func newTestAppWithFormat(f OutputFormat) (*App, func() string) {
	a, out := newTestApp()
	a.Params.Output = f
	return a, out.String
}

func TestParseOutputFormat(t *testing.T) {
	for in, want := range map[string]OutputFormat{"": OutputText, "yaml": OutputYaml, "json": OutputJson} {
		got, err := ParseOutputFormat(in)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
	_, err := ParseOutputFormat("xml")
	assert.Error(t, err)
}

func TestGetQueue_FormatsAgree(t *testing.T) {
	get := func(f OutputFormat) string {
		a, out := newTestAppWithFormat(f)
		a.Params.QueueAPI.Get = func(string) (*api.Queue, error) {
			return &api.Queue{Name: "q1", PriorityFactor: 2}, nil
		}
		require.NoError(t, a.GetQueue("q1"))
		return out()
	}

	// yaml is what the command has always printed
	assert.Equal(t, get(OutputText), get(OutputYaml))

	var fromYaml, fromJson map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(get(OutputYaml)), &fromYaml))
	require.NoError(t, json.Unmarshal([]byte(get(OutputJson)), &fromJson))
	assert.Equal(t, fromYaml, fromJson)
	assert.Equal(t, "Queue", fromJson["kind"])
	assert.Equal(t, "q1", fromJson["name"])
}

func TestGetAllQueues_StructuredIsOneDocument(t *testing.T) {
	for _, f := range []OutputFormat{OutputYaml, OutputJson} {
		a, out := newTestAppWithFormat(f)
		a.Params.QueueAPI.GetAll = func() ([]*api.Queue, error) {
			return []*api.Queue{{Name: "a"}, {Name: "b"}}, nil
		}
		require.NoError(t, a.GetAllQueues(&QueueQueryArgs{}))

		var doc struct {
			Kind   string       `json:"kind"`
			Queues []*api.Queue `json:"queues"`
		}
		require.NoError(t, yaml.Unmarshal([]byte(out()), &doc), string(f))
		assert.Equal(t, "Queue", doc.Kind)
		assert.Equal(t, []string{"a", "b"}, slices.Map(doc.Queues, func(q *api.Queue) string { return q.Name }))
	}
}

func TestGetRetryPolicies_StructuredIsOneDocument(t *testing.T) {
	for _, f := range []OutputFormat{OutputYaml, OutputJson} {
		a, out := newTestAppWithFormat(f)
		a.Params.RetryPolicyAPI.GetAll = func() ([]*api.RetryPolicy, error) {
			return []*api.RetryPolicy{{Name: "p1"}, {Name: "p2"}}, nil
		}
		require.NoError(t, a.GetAllRetryPolicies())

		var doc struct {
			Kind          string             `json:"kind"`
			RetryPolicies []*api.RetryPolicy `json:"retryPolicies"`
		}
		require.NoError(t, yaml.Unmarshal([]byte(out()), &doc), string(f))
		assert.Equal(t, "RetryPolicy", doc.Kind)
		assert.Len(t, doc.RetryPolicies, 2)
	}
}

// get output, in any format, must be usable as create -f input.
func TestGetOutput_RoundTripsThroughCreate(t *testing.T) {
	for _, f := range []OutputFormat{OutputText, OutputYaml, OutputJson} {
		t.Run("queue/"+string(f), func(t *testing.T) {
			want := &api.Queue{
				Name:           "q1",
				PriorityFactor: 2,
				Permissions: []*api.Queue_Permissions{{
					Subjects: []*api.Queue_Permissions_Subject{{Kind: "User", Name: "anonymous"}},
					Verbs:    []string{"submit", "cancel"},
				}},
			}
			a, out := newTestAppWithFormat(f)
			a.Params.QueueAPI.Get = func(string) (*api.Queue, error) { return want, nil }
			require.NoError(t, a.GetQueue("q1"))
			path := filepath.Join(t.TempDir(), "queue")
			require.NoError(t, os.WriteFile(path, []byte(out()), 0o644))

			var created *queue.Queue
			a.Params.QueueAPI.Create = func(q queue.Queue) error { created = &q; return nil }
			require.NoError(t, a.CreateResource(path, false))

			require.NotNil(t, created)
			assert.Equal(t, want.Name, created.Name)
			assert.Equal(t, want.PriorityFactor, float64(created.PriorityFactor))
			require.Len(t, created.Permissions, 1)
			assert.Equal(t, queue.PermissionVerbs{"submit", "cancel"}, created.Permissions[0].Verbs)
			require.Len(t, created.Permissions[0].Subjects, 1)
			assert.Equal(t, "anonymous", created.Permissions[0].Subjects[0].Name)
		})

		t.Run("retry-policy/"+string(f), func(t *testing.T) {
			a, out := newTestAppWithFormat(f)
			a.Params.RetryPolicyAPI.Get = func(name string) (*api.RetryPolicy, error) {
				return &api.RetryPolicy{
					Name:          name,
					RetryLimit:    3,
					DefaultAction: api.RetryAction_RETRY_ACTION_RETRY,
					Rules:         []*api.RetryRule{{Action: api.RetryAction_RETRY_ACTION_FAIL, OnCategory: "UserError"}},
				}, nil
			}
			require.NoError(t, a.GetRetryPolicy("p1"))
			path := filepath.Join(t.TempDir(), "policy")
			require.NoError(t, os.WriteFile(path, []byte(out()), 0o644))

			policy, err := retryPolicyFromFile(path)
			require.NoError(t, err)
			assert.Equal(t, "p1", policy.Name)
			assert.Equal(t, uint32(3), policy.RetryLimit)
			assert.Equal(t, api.RetryAction_RETRY_ACTION_RETRY, policy.DefaultAction)
			require.Len(t, policy.Rules, 1)
			assert.Equal(t, api.RetryAction_RETRY_ACTION_FAIL, policy.Rules[0].Action)
		})
	}
}
