package armadactl

import (
	"encoding/json"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"github.com/armadaproject/armada/internal/lookout/gen/models"
	"github.com/armadaproject/armada/pkg/client/job"
)

// fakeJobs backs the job API with the given job ids and records what it was asked for.
type fakeJobs struct {
	known map[string]bool
	asked []string
}

func newJobTestApp(f OutputFormat, known ...string) (*App, func() string, *fakeJobs) {
	a, out := newTestAppWithFormat(f)
	fake := &fakeJobs{known: map[string]bool{}}
	for _, id := range known {
		fake.known[id] = true
	}
	a.Params.JobAPI.Get = func(id string) (*job.Job, error) {
		fake.asked = append(fake.asked, id)
		if !fake.known[id] {
			return nil, errors.New("not found")
		}
		j := job.NewJob(&models.Job{JobID: id, Queue: "q", State: "RUNNING"})
		j.JobSpec = json.RawMessage(`{"id": "` + id + `"}`)
		return j, nil
	}
	return a, out, fake
}

// document is a get job document.
type document struct {
	APIVersion string          `json:"apiVersion"`
	Kind       string          `json:"kind"`
	JobId      string          `json:"jobId"`
	JobSpec    json.RawMessage `json:"jobSpec"`
}

func parse(t *testing.T, out string) document {
	t.Helper()
	var d document
	require.NoError(t, yaml.Unmarshal([]byte(out), &d), out)
	return d
}

func TestGetJob_IsADocumentWithTheJobAndItsSpec(t *testing.T) {
	for _, f := range []OutputFormat{OutputText, OutputYaml, OutputJson} {
		a, out, fake := newJobTestApp(f, "a")

		require.NoError(t, a.GetJob("a"), string(f))

		d := parse(t, out())
		assert.Equal(t, "Job", d.Kind, string(f))
		assert.NotEmpty(t, d.APIVersion, string(f))
		assert.Equal(t, "a", d.JobId, string(f))
		assert.JSONEq(t, `{"id": "a"}`, string(d.JobSpec), string(f))
		assert.Equal(t, []string{"a"}, fake.asked, string(f))
	}
}

func TestGetJob_DefaultOutputIsTheSameAsYaml(t *testing.T) {
	text, textOut, _ := newJobTestApp(OutputText, "a")
	yamlApp, yamlOut, _ := newJobTestApp(OutputYaml, "a")

	require.NoError(t, text.GetJob("a"))
	require.NoError(t, yamlApp.GetJob("a"))

	assert.Equal(t, yamlOut(), textOut())
}

func TestGetJob_AJobThatIsNotFoundIsAnErrorAndPrintsNothing(t *testing.T) {
	a, out, _ := newJobTestApp(OutputYaml)

	err := a.GetJob("nope")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "error getting job nope: not found")
	assert.Empty(t, out())
}

func TestGetJob_ReportsAFailureOfTheClient(t *testing.T) {
	a, out, _ := newJobTestApp(OutputYaml, "a")
	a.Params.JobAPI.Get = func(string) (*job.Job, error) { return nil, errors.New("lookout is down") }

	err := a.GetJob("a")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "lookout is down")
	assert.Empty(t, out())
}
