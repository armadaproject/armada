package armadactl

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/pkg/api"
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
