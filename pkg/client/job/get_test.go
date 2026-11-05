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

func ids(jobs []*Job) []string {
	out := make([]string, len(jobs))
	for i, j := range jobs {
		out[i] = j.JobID
	}
	return out
}

func TestGet_ReturnsKnownJobsInRequestOrder(t *testing.T) {
	_, url := newFakeLookout(t, "a", "c")

	jobs, err := Get(connectionTo(url))([]string{"c", "unknown", "a"}, false)

	require.NoError(t, err)
	assert.Equal(t, []string{"c", "a"}, ids(jobs))
	assert.Equal(t, "RUNNING", jobs[0].State)
}

func TestGet_FetchesTheJobSpecOnlyWhenAsked(t *testing.T) {
	f, url := newFakeLookout(t, "a")

	jobs, err := Get(connectionTo(url))([]string{"a"}, false)
	require.NoError(t, err)
	assert.Nil(t, jobs[0].JobSpec)
	assert.Equal(t, []string{"/api/v1/jobs"}, f.paths)

	jobs, err = Get(connectionTo(url))([]string{"a"}, true)
	require.NoError(t, err)
	assert.JSONEq(t, `{"id": "a"}`, string(jobs[0].JobSpec))
}

func TestGet_DoesNotFetchTheSpecOfUnknownJobs(t *testing.T) {
	f, url := newFakeLookout(t)

	jobs, err := Get(connectionTo(url))([]string{"nope"}, true)

	require.NoError(t, err)
	assert.Empty(t, jobs)
	assert.NotContains(t, f.paths, "/api/v1/jobSpec")
}

func TestGet_FiltersByAnExactJobId(t *testing.T) {
	f, url := newFakeLookout(t, "a")

	_, err := Get(connectionTo(url))([]string{"a"}, false)

	require.NoError(t, err)
	assert.Equal(t, []any{map[string]any{"field": "jobId", "value": "a", "match": "exact"}}, f.requests[0]["filters"])
	assert.EqualValues(t, 1, f.requests[0]["take"])
}

func TestGet_NeedsALookoutUrl(t *testing.T) {
	_, err := Get(connectionTo(""))([]string{"a"}, false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no Lookout url configured")
}

func TestGet_SendsTheCredentialsOnEveryRequest(t *testing.T) {
	f, url := newFakeLookout(t, "a", "b")
	details := func() (*client.ApiConnectionDetails, error) {
		return &client.ApiConnectionDetails{LookoutUrl: url, BasicAuth: basic.LoginCredentials{Username: "u", Password: "p"}}, nil
	}

	_, err := Get(details)([]string{"a", "b"}, true)

	require.NoError(t, err)
	require.Len(t, f.auth, 4) // a job and a spec for each of the two jobs
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

	_, err := Get(details)([]string{"a"}, false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to send credentials over plain http")
}

func TestGet_ReportsTheStatusAndBodyOfAFailedRequest(t *testing.T) {
	f, url := newFakeLookout(t)
	f.status, f.body = http.StatusInternalServerError, "auth error: bad credentials"

	_, err := Get(connectionTo(url))([]string{"a"}, false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
	assert.Contains(t, err.Error(), "auth error: bad credentials")
}
