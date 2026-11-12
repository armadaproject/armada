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
	known     map[string]bool
	asked     []string
	listed    []job.ListArgs
	listError error
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
	a.Params.JobAPI.List = func(args job.ListArgs) ([]*models.Job, error) {
		fake.listed = append(fake.listed, args)
		if fake.listError != nil {
			return nil, fake.listError
		}
		jobs := []*models.Job{}
		for id := range fake.known {
			jobs = append(jobs, &models.Job{JobID: id, Queue: "q", State: "RUNNING"})
		}
		return jobs, nil
	}
	return a, out, fake
}

// document is a get job or get jobs document, with the jobs of a list as maps to see which keys they have.
type document struct {
	APIVersion string           `json:"apiVersion"`
	Kind       string           `json:"kind"`
	JobId      string           `json:"jobId"`
	JobSpec    json.RawMessage  `json:"jobSpec"`
	Jobs       []map[string]any `json:"jobs"`
}

func parse(t *testing.T, out string) document {
	t.Helper()
	var d document
	require.NoError(t, yaml.Unmarshal([]byte(out), &d), out)
	return d
}

func jobIds(d document) []string {
	ids := make([]string, len(d.Jobs))
	for i, j := range d.Jobs {
		ids[i] = j["jobId"].(string)
	}
	return ids
}

func TestGetJob_IsADocumentWithTheJobAndItsSpec(t *testing.T) {
	for _, f := range []OutputFormat{OutputText, OutputYaml, OutputJson} {
		a, out, fake := newJobTestApp(f, "a")

		require.NoError(t, a.GetJob("a"), string(f))

		d := parse(t, out())
		assert.Equal(t, "Job", d.Kind, string(f))
		assert.NotEmpty(t, d.APIVersion, string(f))
		assert.Equal(t, "a", d.JobId, string(f))
		assert.Nil(t, d.Jobs, string(f))
		assert.JSONEq(t, `{"id": "a"}`, string(d.JobSpec), string(f))
		assert.Equal(t, []string{"a"}, fake.asked, string(f))
	}
}

func TestGetJob_ShowsWhyARunDidNotSucceedInsideTheRun(t *testing.T) {
	for _, f := range []OutputFormat{OutputYaml, OutputJson} {
		a, out, _ := newJobTestApp(f, "a")
		a.Params.JobAPI.Get = func(id string) (*job.Job, error) {
			j := job.NewJob(&models.Job{
				JobID: id, Queue: "q", State: "FAILED",
				Runs: []*models.Run{{RunID: "r1", JobRunState: "RUN_FAILED"}, {RunID: "r2", JobRunState: "RUN_SUCCEEDED"}},
			})
			j.Runs[0].RunDetails = &job.RunDetails{Error: "exit code 3", DebugMessage: "drained"}
			return j, nil
		}

		require.NoError(t, a.GetJob("a"), string(f))

		var doc map[string]any
		require.NoError(t, yaml.Unmarshal([]byte(out()), &doc), string(f))
		assert.NotContains(t, doc, "runDetails", string(f))
		runs := doc["runs"].([]any)
		require.Len(t, runs, 2, string(f))
		assert.Equal(t, "r1", runs[0].(map[string]any)["runId"], string(f))
		assert.Equal(t, map[string]any{"error": "exit code 3", "debugMessage": "drained"}, runs[0].(map[string]any)["runDetails"], string(f))
		assert.NotContains(t, runs[1], "runDetails", string(f))
	}
}

func TestGetJob_ShowsWhyAJobThatNeverRanWasNotRun(t *testing.T) {
	a, out, _ := newJobTestApp(OutputYaml, "a")
	a.Params.JobAPI.Get = func(id string) (*job.Job, error) {
		j := job.NewJob(&models.Job{JobID: id, Queue: "q", State: "REJECTED"})
		j.Error = "requests resources which exceed the limit"
		return j, nil
	}

	require.NoError(t, a.GetJob("a"))

	var doc map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(out()), &doc))
	assert.Equal(t, "requests resources which exceed the limit", doc["error"])
	assert.Equal(t, []any{}, doc["runs"])
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

func TestListJobs_IsADocumentWithAListOfJobsWithoutSpecs(t *testing.T) {
	for _, f := range []OutputFormat{OutputText, OutputYaml, OutputJson} {
		a, out, _ := newJobTestApp(f, "a", "b")

		require.NoError(t, a.ListJobs(job.ListArgs{Take: 10}), string(f))

		d := parse(t, out())
		assert.Equal(t, "Job", d.Kind, string(f))
		assert.ElementsMatch(t, []string{"a", "b"}, jobIds(d), string(f))
		for _, j := range d.Jobs {
			assert.NotContains(t, j, "jobSpec", string(f))
		}
	}
}

func TestListJobs_NothingFoundIsAnEmptyList(t *testing.T) {
	for _, f := range []OutputFormat{OutputText, OutputYaml, OutputJson} {
		a, out, _ := newJobTestApp(f)

		require.NoError(t, a.ListJobs(job.ListArgs{Take: 10}), string(f))

		var raw map[string]any
		require.NoError(t, yaml.Unmarshal([]byte(out()), &raw), string(f))
		assert.Equal(t, []any{}, raw["jobs"], string(f)) // a list, not null or missing
	}
}

func TestListJobs_PassesTheArgsOn(t *testing.T) {
	a, _, fake := newJobTestApp(OutputYaml)
	args := job.ListArgs{
		Filters: []*models.Filter{{Field: "queue", Match: "exact", Value: "q"}},
		Order:   models.Order{Field: "submitted", Direction: "ASC"},
		Skip:    3,
		Take:    7,
	}

	require.NoError(t, a.ListJobs(args))

	assert.Equal(t, []job.ListArgs{args}, fake.listed)
}

func TestListJobs_ReportsAFailureOfTheClient(t *testing.T) {
	a, out, fake := newJobTestApp(OutputYaml)
	fake.listError = errors.New("lookout is down")

	err := a.ListJobs(job.ListArgs{Take: 10})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "lookout is down")
	assert.Empty(t, out())
}
