package reports

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	internalschedulerobjects "github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api/schedulerobjects"
)

// The REST gateway must write the heartbeat as a date-time string, as the OpenAPI schema says.
func TestServer_GetExecutors_Http(t *testing.T) {
	heartbeat := time.Date(2026, 11, 24, 10, 0, 0, 500, time.UTC)
	repo := &fakeExecutorRepository{
		executors: []*internalschedulerobjects.Executor{
			{Id: "executor-a", Pool: "cpu", LastUpdateTime: protoutil.ToTimestamp(heartbeat)},
			{Id: "executor-b", Pool: "gpu"},
		},
	}
	gw := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, new(runtime.JSONBuiltin)))
	require.NoError(t, schedulerobjects.RegisterSchedulerReportingHandlerServer(context.Background(), gw, NewServer(NewSchedulingContextRepository(), repo)))

	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/executors", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var body struct {
		Executors []map[string]any `json:"executors"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Executors, 2)
	assert.Equal(t, "executor-a", body.Executors[0]["name"])
	lastUpdate, ok := body.Executors[0]["lastUpdateTime"].(string)
	require.True(t, ok, "lastUpdateTime should be a string, got %T", body.Executors[0]["lastUpdateTime"])
	parsed, err := time.Parse(time.RFC3339Nano, lastUpdate)
	require.NoError(t, err)
	assert.True(t, parsed.Equal(heartbeat))
	assert.NotContains(t, body.Executors[1], "lastUpdateTime")
}
