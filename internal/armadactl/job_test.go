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
	asked     [][]string
	withSpec  []bool
	listed    []job.ListArgs
	listError error
}

func newJobTestApp(f OutputFormat, known ...string) (*App, func() string, *fakeJobs) {
	a, out := newTestAppWithFormat(f)
	fake := &fakeJobs{known: map[string]bool{}}
	for _, id := range known {
		fake.known[id] = true
	}
	a.Params.JobAPI.Get = func(ids []string, withSpec bool) ([]*job.Job, error) {
		fake.asked = append(fake.asked, ids)
		fake.withSpec = append(fake.withSpec, withSpec)
		var jobs []*job.Job
		for _, id := range ids {
			if !fake.known[id] {
				continue
			}
			j := &job.Job{Job: &models.Job{JobID: id, Queue: "q", State: "RUNNING"}}
			if withSpec {
				j.JobSpec = json.RawMessage(`{"id": "` + id + `"}`)
			}
			jobs = append(jobs, j)
		}
		return jobs, nil
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

func TestGetJobs_ASingleJobIsADocumentOfItsOwn(t *testing.T) {
	for _, f := range []OutputFormat{OutputText, OutputYaml, OutputJson} {
		a, out, fake := newJobTestApp(f, "a")

		require.NoError(t, a.GetJobs([]string{"a"}), string(f))

		d := parse(t, out())
		assert.Equal(t, "Job", d.Kind, string(f))
		assert.NotEmpty(t, d.APIVersion, string(f))
		assert.Equal(t, "a", d.JobId, string(f))
		assert.Nil(t, d.Jobs, string(f))
		// The details always include the spec.
		assert.JSONEq(t, `{"id": "a"}`, string(d.JobSpec), string(f))
		assert.Equal(t, []bool{true}, fake.withSpec, string(f))
	}
}

func TestGetJobs_SeveralJobsAreAListInTheOrderGiven(t *testing.T) {
	a, out, _ := newJobTestApp(OutputYaml, "a", "b", "c")

	require.NoError(t, a.GetJobs([]string{"c", "a", "b"}))

	d := parse(t, out())
	assert.Equal(t, "Job", d.Kind)
	assert.Empty(t, d.JobId)
	assert.Equal(t, []string{"c", "a", "b"}, jobIds(d))
	for _, j := range d.Jobs {
		assert.Contains(t, j, "jobSpec")
	}
}

func TestGetJobs_DefaultOutputIsTheSameAsYaml(t *testing.T) {
	text, textOut, _ := newJobTestApp(OutputText, "a", "b")
	yamlApp, yamlOut, _ := newJobTestApp(OutputYaml, "a", "b")

	require.NoError(t, text.GetJobs([]string{"a", "b"}))
	require.NoError(t, yamlApp.GetJobs([]string{"a", "b"}))

	assert.Equal(t, yamlOut(), textOut())
}

func TestGetJobs_AskForEachJobOnce(t *testing.T) {
	a, out, fake := newJobTestApp(OutputYaml, "a", "b")

	require.NoError(t, a.GetJobs([]string{"a", "b", "a", "b", "a"}))

	assert.Equal(t, [][]string{{"a", "b"}}, fake.asked)
	assert.Equal(t, []string{"a", "b"}, jobIds(parse(t, out())))
}

func TestGetJobs_ASingleJobThatIsNotFoundIsAnErrorAndPrintsNothing(t *testing.T) {
	a, out, _ := newJobTestApp(OutputYaml)

	err := a.GetJobs([]string{"nope"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "job nope not found")
	assert.Empty(t, out())
}

func TestGetJobs_SeveralJobsPrintTheOnesFoundThenReportTheOnesMissing(t *testing.T) {
	a, out, _ := newJobTestApp(OutputJson, "a", "c")

	err := a.GetJobs([]string{"a", "nope", "c", "gone"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "jobs not found: nope, gone")
	var d document
	require.NoError(t, json.Unmarshal([]byte(out()), &d), "the output is still valid json")
	assert.Equal(t, []string{"a", "c"}, jobIds(d))
}

func TestGetJobs_NoneFoundOfSeveralPrintsAnEmptyList(t *testing.T) {
	a, out, _ := newJobTestApp(OutputYaml)

	err := a.GetJobs([]string{"x", "y"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "jobs not found: x, y")
	assert.Contains(t, out(), "jobs: []")
}

func TestGetJobs_ReportsAFailureOfTheClient(t *testing.T) {
	a, out, _ := newJobTestApp(OutputYaml, "a")
	a.Params.JobAPI.Get = func([]string, bool) ([]*job.Job, error) { return nil, errors.New("lookout is down") }

	err := a.GetJobs([]string{"a"})

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
