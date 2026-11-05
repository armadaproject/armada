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
