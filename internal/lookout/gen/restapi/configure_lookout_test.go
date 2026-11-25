package restapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordRequestDuration_RecordsStatusCode(t *testing.T) {
	handler := recordRequestDuration(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/jobs":
			_, _ = w.Write([]byte("[]"))
		case "/api/v1/jobGroups":
			w.WriteHeader(http.StatusBadRequest)
			// A second WriteHeader is ignored by net/http, the first code is what the client sees.
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	for _, req := range []struct{ method, path string }{
		{http.MethodPost, "/api/v1/jobs"},
		{http.MethodPost, "/api/v1/jobs"},
		{http.MethodPost, "/api/v1/jobGroups"},
		{http.MethodGet, "/health"},
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(req.method, req.path, nil))
	}

	expected := `
# HELP lookout_requests_total Number of requests handled, by response status code
# TYPE lookout_requests_total counter
lookout_requests_total{code="200",endpoint="/api/v1/jobs",method="POST",user="anonymous"} 2
lookout_requests_total{code="400",endpoint="/api/v1/jobGroups",method="POST",user="anonymous"} 1
`
	require.NoError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(expected), "lookout_requests_total"))
}

func TestStatusRecorder_Unwrap(t *testing.T) {
	rec := httptest.NewRecorder()
	recorder := &statusRecorder{ResponseWriter: rec, status: http.StatusOK}
	assert.Equal(t, http.ResponseWriter(rec), recorder.Unwrap())
	require.NoError(t, http.NewResponseController(recorder).Flush())
	assert.True(t, rec.Flushed)
}
