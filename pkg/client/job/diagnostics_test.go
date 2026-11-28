package job

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/lookout/gen/models"
)

func run(id, state string) map[string]any {
	return map[string]any{"runId": id, "cluster": "c", "jobRunState": state}
}

// asked returns the values of key in the requests made to path, in order.
func (f *fakeLookout) asked(path, key string) []any {
	var values []any
	for i, p := range f.paths {
		if p == path {
			values = append(values, f.requests[i][key])
		}
	}
	return values
}

// document is how the job is written: as the JSON that becomes yaml or json.
func document(t *testing.T, job *Job) map[string]any {
	t.Helper()
	b, err := json.Marshal(job)
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(b, &doc))
	return doc
}

func TestGet_ARunThatSucceededIsNotAskedWhyItDidNotSucceed(t *testing.T) {
	f, url := newFakeLookout(t, "a")
	f.states["a"] = "SUCCEEDED"
	f.runs["a"] = []map[string]any{run("r1", "RUN_SUCCEEDED")}

	job, err := Get(connectionTo(url))("a")

	require.NoError(t, err)
	assert.Equal(t, []string{"/api/v1/jobs", "/api/v1/jobSpec"}, f.paths)
	assert.Empty(t, job.Error)
	assert.Nil(t, job.Runs[0].RunDetails)
}

func TestGet_ARunThatDidNotSucceedHasEverythingLookoutRecordedAboutIt(t *testing.T) {
	f, url := newFakeLookout(t, "a")
	f.states["a"] = "FAILED"
	f.runs["a"] = []map[string]any{run("r1", "RUN_FAILED")}
	f.jobErrors["a"] = "recorded for the job, but only a rejected job is asked for it"
	f.runErrors["r1"] = "Container sleep failed with exit code 3"
	f.debugMessages["r1"] = "the node was drained"
	f.terminationReasons["r1"] = "preempted for a higher priority job"

	job, err := Get(connectionTo(url))("a")

	require.NoError(t, err)
	assert.Equal(t, &RunDetails{
		Error:                      "Container sleep failed with exit code 3",
		DebugMessage:               "the node was drained",
		SchedulerTerminationReason: "preempted for a higher priority job",
	}, job.Runs[0].RunDetails)
	assert.Empty(t, job.Error)
	assert.Empty(t, f.asked("/api/v1/jobError", "jobId"))
	for _, path := range []string{"/api/v1/jobRunError", "/api/v1/jobRunDebugMessage", "/api/v1/jobRunSchedulerTerminationReason"} {
		assert.Equal(t, []any{"r1"}, f.asked(path, "runId"), path)
	}
}

// The details are written inside the run they are about, next to the rest of what is known of it.
func TestGet_TheDetailsOfARunAreWrittenInsideTheRun(t *testing.T) {
	f, url := newFakeLookout(t, "a")
	f.states["a"] = "FAILED"
	f.runs["a"] = []map[string]any{run("r1", "RUN_FAILED"), run("r2", "RUN_SUCCEEDED")}
	f.runErrors["r1"] = "Container sleep failed with exit code 3"

	job, err := Get(connectionTo(url))("a")

	require.NoError(t, err)
	doc := document(t, job)
	assert.NotContains(t, doc, "error")
	assert.NotContains(t, doc, "runDetails")
	runs := doc["runs"].([]any)
	require.Len(t, runs, 2)
	failed := runs[0].(map[string]any)
	assert.Equal(t, "r1", failed["runId"])
	assert.Equal(t, "RUN_FAILED", failed["jobRunState"])
	// What Lookout has nothing for is left out, which here is the debug message and the termination reason.
	assert.Equal(t, map[string]any{"error": "Container sleep failed with exit code 3"}, failed["runDetails"])
	assert.NotContains(t, runs[1], "runDetails")
}

func TestGet_ARunWithNothingRecordedHasNoDetails(t *testing.T) {
	f, url := newFakeLookout(t, "a")
	f.states["a"] = "FAILED"
	f.runs["a"] = []map[string]any{run("r1", "RUN_FAILED")}

	job, err := Get(connectionTo(url))("a")

	require.NoError(t, err)
	assert.Nil(t, job.Runs[0].RunDetails)
	assert.NotContains(t, document(t, job)["runs"].([]any)[0], "runDetails")
}

// Lookout answers an empty object for the debug message of some runs, and a 400 that says not found for others.
func TestGet_ADebugMessageThatIsNotFoundIsLeftOutToo(t *testing.T) {
	f, url := newFakeLookout(t, "a")
	f.debugMessageNotFound = true
	f.states["a"] = "PREEMPTED"
	f.runs["a"] = []map[string]any{run("r1", "RUN_PREEMPTED")}
	f.runErrors["r1"] = "Preempted by admin"

	job, err := Get(connectionTo(url))("a")

	require.NoError(t, err)
	assert.Equal(t, &RunDetails{Error: "Preempted by admin"}, job.Runs[0].RunDetails)
}

func TestGet_OnlyAsksAboutRunsThatDidNotSucceed(t *testing.T) {
	f, url := newFakeLookout(t, "a")
	f.states["a"] = "FAILED"
	runs := []string{"failed", "done", "going", "waiting", "leased", "preempted"}
	states := []string{"RUN_FAILED", "RUN_SUCCEEDED", "RUN_RUNNING", "RUN_PENDING", "RUN_LEASED", "RUN_PREEMPTED"}
	for i, id := range runs {
		f.runs["a"] = append(f.runs["a"], run(id, states[i]))
		f.runErrors[id] = "an error"
	}

	job, err := Get(connectionTo(url))("a")

	require.NoError(t, err)
	assert.Equal(t, []any{"failed", "preempted"}, f.asked("/api/v1/jobRunError", "runId"))
	for i, r := range job.Runs {
		assert.Equal(t, runs[i] == "failed" || runs[i] == "preempted", r.RunDetails != nil, runs[i])
	}
}

// Lookout records an error for a job itself only when it rejected it. A failed or preempted job has its reasons on its runs.
func TestGet_AsksForTheErrorOfAJobOnlyWhenItWasRejected(t *testing.T) {
	for _, state := range []string{"QUEUED", "LEASED", "PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED", "PREEMPTED", "REJECTED"} {
		for _, runs := range [][]map[string]any{nil, {run("r1", "RUN_FAILED")}} {
			f, url := newFakeLookout(t, "a")
			f.states["a"] = state
			f.runs["a"] = runs

			_, err := Get(connectionTo(url))("a")

			require.NoError(t, err, state)
			assert.Equal(t, state == "REJECTED", len(f.asked("/api/v1/jobError", "jobId")) == 1, "state %s, %d runs", state, len(runs))
		}
	}
}

func TestGet_AJobThatNeverRanShowsItsOwnError(t *testing.T) {
	f, url := newFakeLookout(t, "a")
	f.states["a"] = "REJECTED"
	f.jobErrors["a"] = "requests resources which exceed the limit"

	job, err := Get(connectionTo(url))("a")

	require.NoError(t, err)
	assert.Equal(t, "requests resources which exceed the limit", job.Error)
	doc := document(t, job)
	assert.Equal(t, "requests resources which exceed the limit", doc["error"])
	assert.Equal(t, []any{}, doc["runs"], "an empty list, not null")
}

func TestGet_ARejectedJobWithNothingRecordedHasNoError(t *testing.T) {
	f, url := newFakeLookout(t, "a")
	f.states["a"] = "REJECTED"

	job, err := Get(connectionTo(url))("a")

	require.NoError(t, err)
	assert.Empty(t, job.Error)
	assert.NotContains(t, document(t, job), "error")
}

func TestGet_AFailureThatIsNotNothingRecordedIsAnError(t *testing.T) {
	for path, state := range map[string]string{
		"/api/v1/jobError":                         "REJECTED", // only a rejected job is asked for its error
		"/api/v1/jobRunError":                      "FAILED",
		"/api/v1/jobRunDebugMessage":               "FAILED",
		"/api/v1/jobRunSchedulerTerminationReason": "FAILED",
	} {
		f, url := newFakeLookout(t, "a")
		f.states["a"] = state
		if state != "REJECTED" {
			f.runs["a"] = []map[string]any{run("r1", "RUN_FAILED")}
		}
		f.failing[path] = true

		job, err := Get(connectionTo(url))("a")

		require.Error(t, err, path)
		assert.Contains(t, err.Error(), "500", path)
		assert.Contains(t, err.Error(), "boom", path)
		assert.Nil(t, job, path)
	}
}

// The runs are replaced by ones that can carry details, so a job has to be made so that they are the runs it had.
func TestNewJob_KeepsTheRunsOfTheJob(t *testing.T) {
	model := &models.Job{
		JobID: "a",
		Runs:  []*models.Run{{RunID: "r1", Cluster: "c", JobRunState: "RUN_FAILED"}, {RunID: "r2", Cluster: "c", JobRunState: "RUN_SUCCEEDED"}},
	}

	doc := document(t, NewJob(model))

	runs := doc["runs"].([]any)
	require.Len(t, runs, 2)
	assert.Equal(t, "r1", runs[0].(map[string]any)["runId"])
	assert.Equal(t, "RUN_SUCCEEDED", runs[1].(map[string]any)["jobRunState"])
	assert.Equal(t, "a", doc["jobId"])
}

func TestNewJob_AJobWithoutRunsHasAnEmptyList(t *testing.T) {
	assert.Equal(t, []any{}, document(t, NewJob(&models.Job{JobID: "a"}))["runs"])
}
