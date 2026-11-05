package job

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/auth/basic"
)

// fakeLookout serves the two Lookout endpoints the client uses, for a set of known jobs, and records the requests.
type fakeLookout struct {
	known map[string]bool
	// status and body, when set, are the response to every request.
	status int
	body   string

	mu       sync.Mutex
	paths    []string
	auth     []string
	requests []map[string]any
}

func newFakeLookout(t *testing.T, known ...string) (*fakeLookout, string) {
	t.Helper()
	f := &fakeLookout{known: map[string]bool{}}
	for _, id := range known {
		f.known[id] = true
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv.URL
}

func (f *fakeLookout) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var request map[string]any
	_ = json.NewDecoder(r.Body).Decode(&request)

	f.mu.Lock()
	f.paths = append(f.paths, r.URL.Path)
	f.auth = append(f.auth, r.Header.Get("authorization"))
	f.requests = append(f.requests, request)
	f.mu.Unlock()

	if f.status != 0 {
		w.WriteHeader(f.status)
		_, _ = w.Write([]byte(f.body))
		return
	}
	switch r.URL.Path {
	case "/api/v1/jobs":
		filters, _ := request["filters"].([]any)
		var jobs []map[string]any
		for _, filter := range filters {
			filter := filter.(map[string]any)
			if filter["field"] == "jobId" {
				if id, _ := filter["value"].(string); f.known[id] {
					jobs = append(jobs, map[string]any{"jobId": id, "queue": "q", "jobSet": "js", "state": "RUNNING"})
				}
			}
		}
		// Lookout answers null, not an empty list, when nothing matches.
		_ = json.NewEncoder(w).Encode(map[string]any{"jobs": jobs})
	case "/api/v1/jobSpec":
		_ = json.NewEncoder(w).Encode(map[string]any{"job": map[string]any{"id": request["jobId"]}})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func connectionTo(url string) client.ConnectionDetails {
	return func() (*client.ApiConnectionDetails, error) {
		return &client.ApiConnectionDetails{LookoutUrl: url}, nil
	}
}

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

// Resolving the credentials can log in, which may be interactive, so it happens once for all the requests.
func TestNewLookout_ResolvesTheCredentialsOnce(t *testing.T) {
	f, url := newFakeLookout(t, "a", "b")
	resolved := 0
	var resolvedFor string
	l, err := newLookout(&client.ApiConnectionDetails{LookoutUrl: url}, func(_ context.Context, url string) (map[string]string, error) {
		resolved++
		resolvedFor = url
		return map[string]string{"authorization": "token"}, nil
	})
	require.NoError(t, err)

	for _, id := range []string{"a", "b"} {
		_, err = l.getJob(id)
		require.NoError(t, err)
		_, err = l.getJobSpec(id)
		require.NoError(t, err)
	}

	assert.Equal(t, 1, resolved)
	assert.Equal(t, url, resolvedFor)
	require.Len(t, f.auth, 4)
	for _, got := range f.auth {
		assert.Equal(t, "token", got)
	}
}

func TestNewLookout_ReportsAFailureToResolveTheCredentials(t *testing.T) {
	_, err := newLookout(&client.ApiConnectionDetails{LookoutUrl: "localhost:8089"}, func(context.Context, string) (map[string]string, error) {
		return nil, assert.AnError
	})

	assert.ErrorIs(t, err, assert.AnError)
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

func TestList_SendsTheRequest(t *testing.T) {
	f, url := newFakeLookout(t)
	args := ListArgs{
		Filters:       []Filter{{Field: "queue", Match: "anyOf", Value: []string{"a", "b"}}, {Field: "team", Match: "exact", Value: "data", IsAnnotation: true}},
		Order:         Order{Field: "queue", Direction: "ASC"},
		ActiveJobSets: true,
		Skip:          5,
		Take:          20,
	}

	_, err := List(connectionTo(url))(args)

	require.NoError(t, err)
	r := f.requests[0]
	assert.Equal(t, []any{
		map[string]any{"field": "queue", "match": "anyOf", "value": []any{"a", "b"}},
		map[string]any{"field": "team", "match": "exact", "value": "data", "isAnnotation": true},
	}, r["filters"])
	assert.Equal(t, map[string]any{"field": "queue", "direction": "ASC"}, r["order"])
	assert.Equal(t, true, r["activeJobSets"])
	assert.EqualValues(t, 5, r["skip"])
	assert.EqualValues(t, 20, r["take"])
}

// Lookout treats null filters as matching nothing, so no filters must be sent as an empty list.
func TestList_SendsNoFiltersAsAnEmptyList(t *testing.T) {
	f, url := newFakeLookout(t)

	_, err := List(connectionTo(url))(ListArgs{Order: Order{Field: "submitted", Direction: "DESC"}, Take: 1})

	require.NoError(t, err)
	assert.Equal(t, []any{}, f.requests[0]["filters"])
}

func TestList_ReturnsAnEmptyListWhenNothingMatches(t *testing.T) {
	_, url := newFakeLookout(t)

	jobs, err := List(connectionTo(url))(ListArgs{Take: 1})

	require.NoError(t, err)
	assert.NotNil(t, jobs)
	assert.Empty(t, jobs)
}

func TestFilter_LeavesOutAnAbsentValue(t *testing.T) {
	b, err := json.Marshal(Filter{Field: "team", Match: "exists", IsAnnotation: true})

	require.NoError(t, err)
	assert.JSONEq(t, `{"field": "team", "match": "exists", "isAnnotation": true}`, string(b))
}

func TestBaseUrl(t *testing.T) {
	for in, want := range map[string]string{
		"localhost:8089":                 "http://localhost:8089",
		"127.0.0.1:8089":                 "http://127.0.0.1:8089",
		"lookout.example.com":            "https://lookout.example.com",
		"localhost.evil.com":             "https://localhost.evil.com",
		"http://lookout.example.com":     "http://lookout.example.com",
		"https://lookout.example.com/":   "https://lookout.example.com",
		"https://lookout.example.com///": "https://lookout.example.com",
	} {
		assert.Equal(t, want, baseUrl(in), in)
	}
}
