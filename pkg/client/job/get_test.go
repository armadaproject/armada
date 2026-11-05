package job

import (
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/auth/basic"
)

func TestGet_ReturnsTheJobWithItsSpec(t *testing.T) {
	f, url := newFakeLookout(t, "a")
	f.runs["a"] = []map[string]any{run("r1", "RUN_RUNNING")} // a job that ran is not asked for an error of its own

	job, err := Get(connectionTo(url))("a")

	require.NoError(t, err)
	assert.Equal(t, "a", job.JobID)
	assert.Equal(t, "RUNNING", job.State)
	assert.JSONEq(t, `{"id": "a"}`, string(job.JobSpec))
	assert.Equal(t, []string{"/api/v1/jobs", "/api/v1/jobSpec"}, f.paths)
}

func TestGet_AJobLookoutDoesNotKnowIsAnErrorAndItsSpecIsNotFetched(t *testing.T) {
	f, url := newFakeLookout(t)

	job, err := Get(connectionTo(url))("nope")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	assert.Nil(t, job)
	assert.NotContains(t, f.paths, "/api/v1/jobSpec")
}

func TestGet_FiltersByAnExactJobId(t *testing.T) {
	f, url := newFakeLookout(t, "a")

	_, err := Get(connectionTo(url))("a")

	require.NoError(t, err)
	assert.Equal(t, []any{map[string]any{"field": "jobId", "value": "a", "match": "exact"}}, f.requests[0]["filters"])
	assert.EqualValues(t, 1, f.requests[0]["take"])
}

func TestGet_NeedsALookoutUrl(t *testing.T) {
	_, err := Get(connectionTo(""))("a")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no Lookout url configured")
}

func TestGet_SendsTheCredentialsOnEveryRequest(t *testing.T) {
	f, url := newFakeLookout(t, "a")
	f.runs["a"] = []map[string]any{run("r1", "RUN_RUNNING")}
	details := func() (*client.ApiConnectionDetails, error) {
		return &client.ApiConnectionDetails{LookoutUrl: url, BasicAuth: basic.LoginCredentials{Username: "u", Password: "p"}}, nil
	}

	_, err := Get(details)("a")

	require.NoError(t, err)
	require.Len(t, f.auth, 2) // the job and its spec
	want := "basic " + base64.StdEncoding.EncodeToString([]byte("u:p"))
	for _, got := range f.auth {
		assert.Equal(t, want, got)
	}
}

func TestGet_RefusesToSendCredentialsOverPlainHttpToARemoteHost(t *testing.T) {
	details := func() (*client.ApiConnectionDetails, error) {
		return &client.ApiConnectionDetails{
			LookoutUrl: "http://lookout.example.com",
			BasicAuth:  basic.LoginCredentials{Username: "u", Password: "p"},
		}, nil
	}

	_, err := Get(details)("a")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to send credentials over plain http")
}

func TestGet_ReportsTheStatusAndBodyOfAFailedRequest(t *testing.T) {
	f, url := newFakeLookout(t)
	f.status, f.body = http.StatusInternalServerError, "auth error: bad credentials"

	_, err := Get(connectionTo(url))("a")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
	assert.Contains(t, err.Error(), "auth error: bad credentials")
}
