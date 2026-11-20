package job

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/armadaproject/armada/pkg/client"
)

// fakeLookout serves the Lookout endpoints the client uses, for a set of known jobs, and records the requests.
type fakeLookout struct {
	known map[string]bool
	// states and runs say what a known job looks like: RUNNING and with no runs unless they are given here.
	states map[string]string
	runs   map[string][]map[string]any
	// What Lookout has recorded, by job id or by run id. What is not here is answered as Lookout answers it.
	jobErrors, runErrors, debugMessages, terminationReasons map[string]string
	// debugMessageNotFound makes a run without a debug message get a 400 that says not found, as Lookout does for some
	// runs, instead of the empty object it answers for others.
	debugMessageNotFound bool
	// failing are the paths that answer 500.
	failing map[string]bool
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
	f := &fakeLookout{
		known: map[string]bool{}, states: map[string]string{}, runs: map[string][]map[string]any{}, failing: map[string]bool{},
		jobErrors: map[string]string{}, runErrors: map[string]string{}, debugMessages: map[string]string{}, terminationReasons: map[string]string{},
	}
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
	if f.failing[r.URL.Path] {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
		return
	}
	switch r.URL.Path {
	case "/api/v1/jobs":
		filters, _ := request["filters"].([]any)
		// Lookout checks every request against its swagger schema before it looks at it, and that schema
		// requires each filter to have a value, so a filter without one is rejected as it is there.
		for i, filter := range filters {
			if filter.(map[string]any)["value"] == nil {
				w.WriteHeader(http.StatusUnprocessableEntity)
				_, _ = fmt.Fprintf(w, `{"code":602,"message":"filters.%d.value in body is required"}`, i)
				return
			}
		}
		var jobs []map[string]any
		for _, filter := range filters {
			filter := filter.(map[string]any)
			if filter["field"] == "jobId" {
				if id, _ := filter["value"].(string); f.known[id] {
					state := f.states[id]
					if state == "" {
						state = "RUNNING"
					}
					jobs = append(jobs, map[string]any{"jobId": id, "queue": "q", "jobSet": "js", "state": state, "runs": f.runs[id]})
				}
			}
		}
		// Lookout answers null, not an empty list, when nothing matches.
		_ = json.NewEncoder(w).Encode(map[string]any{"jobs": jobs})
	case "/api/v1/jobSpec":
		_ = json.NewEncoder(w).Encode(map[string]any{"job": map[string]any{"id": request["jobId"]}})
	case "/api/v1/jobError":
		f.recorded(w, f.jobErrors, request["jobId"], "errorString", "no error found for job with id %v: not found", true)
	case "/api/v1/jobRunError":
		f.recorded(w, f.runErrors, request["runId"], "errorString", "no error found for run with id %v: not found", true)
	case "/api/v1/jobRunDebugMessage":
		f.recorded(w, f.debugMessages, request["runId"], "errorString", "no debug message found for run with id %v: not found", f.debugMessageNotFound)
	case "/api/v1/jobRunSchedulerTerminationReason":
		f.recorded(w, f.terminationReasons, request["runId"], "schedulerTerminationReason", "no scheduler termination reason found for run with id %v: not found", true)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// recorded answers with what was recorded for id, and for what was not, as Lookout does: a 400 that says not found,
// or an empty object.
func (f *fakeLookout) recorded(w http.ResponseWriter, recorded map[string]string, id any, key, notFound string, answersNotFound bool) {
	if message, ok := recorded[fmt.Sprint(id)]; ok {
		_ = json.NewEncoder(w).Encode(map[string]string{key: message})
		return
	}
	if answersNotFound {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, `{"error":%q}`, fmt.Sprintf(notFound, id))
		return
	}
	_, _ = w.Write([]byte(`{}`))
}

func connectionTo(url string) client.ConnectionDetails {
	return func() (*client.ApiConnectionDetails, error) {
		return &client.ApiConnectionDetails{LookoutUrl: url}, nil
	}
}
