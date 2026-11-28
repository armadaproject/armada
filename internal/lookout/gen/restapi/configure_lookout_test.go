package restapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/auth"
)

// requestCount returns the current value of lookout_requests_total for one label set. The counter is
// package-global, so the tests compare counts before and after instead of absolute totals.
func requestCount(t *testing.T, method, endpoint, code string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "lookout_requests_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["method"] == method && labels["endpoint"] == endpoint && labels["code"] == code {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

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

	jobsBefore := requestCount(t, "POST", "/api/v1/jobs", "200")
	groupsBefore := requestCount(t, "POST", "/api/v1/jobGroups", "400")
	healthBefore := requestCount(t, "GET", "/health", "404")

	for _, req := range []struct{ method, path string }{
		{http.MethodPost, "/api/v1/jobs"},
		{http.MethodPost, "/api/v1/jobs"},
		{http.MethodPost, "/api/v1/jobGroups"},
		{http.MethodGet, "/health"},
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(req.method, req.path, nil))
	}

	assert.Equal(t, float64(2), requestCount(t, "POST", "/api/v1/jobs", "200")-jobsBefore)
	assert.Equal(t, float64(1), requestCount(t, "POST", "/api/v1/jobGroups", "400")-groupsBefore)
	assert.Equal(t, float64(0), requestCount(t, "GET", "/health", "404")-healthBefore, "/health is not an /api/v1/ path")
}

func TestRecordRequest_UnknownMethodsShareOneLabel(t *testing.T) {
	handler := recordRequestDuration(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	before := requestCount(t, "other", "/api/v1/jobs", "200")
	for _, method := range []string{"FOO", "BAR", "BAZ"} {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, "/api/v1/jobs", nil))
	}

	assert.Equal(t, float64(3), requestCount(t, "other", "/api/v1/jobs", "200")-before)
	assert.Equal(t, float64(0), requestCount(t, "FOO", "/api/v1/jobs", "200"))
}

func TestStatusRecorder_Unwrap(t *testing.T) {
	rec := httptest.NewRecorder()
	recorder := &statusRecorder{ResponseWriter: rec, status: http.StatusOK}
	assert.Equal(t, http.ResponseWriter(rec), recorder.Unwrap())
	require.NoError(t, http.NewResponseController(recorder).Flush())
	assert.True(t, rec.Flushed)
}

func TestRecordRequest_UnknownPathsShareOneLabel(t *testing.T) {
	handler := recordRequestDuration(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	before := requestCount(t, "GET", "unknown", "404")
	for _, path := range []string{"/api/v1/foo", "/api/v1/bar", "/api/v1/jobs/extra"} {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}

	assert.Equal(t, float64(3), requestCount(t, "GET", "unknown", "404")-before)
	assert.Equal(t, float64(0), requestCount(t, "GET", "/api/v1/foo", "404"))
}

type failingAuthService struct{}

func (failingAuthService) Authenticate(_ context.Context, _ string) (auth.Principal, error) {
	return nil, errors.New("bad credentials")
}

func TestRecordRequest_CountsFailedAuthentication(t *testing.T) {
	previous := authService
	authService = failingAuthService{}
	t.Cleanup(func() { authService = previous })

	called := false
	handler := recordRequestDuration(authHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})))

	before := requestCount(t, "GET", "/api/v1/jobs", "500")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil))

	assert.False(t, called)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Equal(t, float64(1), requestCount(t, "GET", "/api/v1/jobs", "500")-before)
}
