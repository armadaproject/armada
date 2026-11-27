package reports

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gogo/status"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	internalschedulerobjects "github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api/schedulerobjects"
)

func TestServer_GetExecutors(t *testing.T) {
	heartbeat := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	internalExecutorA := &internalschedulerobjects.Executor{
		Id:             "executor-a",
		Pool:           "cpu",
		LastUpdateTime: protoutil.ToTimestamp(heartbeat),
	}
	internalExecutorB := &internalschedulerobjects.Executor{
		Id:             "executor-b",
		Pool:           "gpu",
		LastUpdateTime: protoutil.ToTimestamp(heartbeat),
		Nodes:          []*internalschedulerobjects.Node{{Id: "node-1"}, {Id: "node-2"}},
	}
	// An uncordoned executor keeps the user who last changed it, which must not be reported.
	settingsA := &internalschedulerobjects.ExecutorSettings{ExecutorId: "executor-a", Cordoned: false, CordonReason: "old reason", SetByUser: "bob"}
	settingsB := &internalschedulerobjects.ExecutorSettings{ExecutorId: "executor-b", Cordoned: true, CordonReason: "maintenance", SetByUser: "alice"}
	// Settings can outlive the executor, these must not show up on their own.
	settingsGone := &internalschedulerobjects.ExecutorSettings{ExecutorId: "executor-gone", Cordoned: true, CordonReason: "retired", SetByUser: "bob"}

	executorA := &schedulerobjects.ExecutorSummary{
		Name:           "executor-a",
		Pool:           "cpu",
		LastUpdateTime: protoutil.ToTimestamp(heartbeat),
	}
	executorB := &schedulerobjects.ExecutorSummary{
		Name:           "executor-b",
		Pool:           "gpu",
		LastUpdateTime: protoutil.ToTimestamp(heartbeat),
		NodeCount:      2,
		Cordoned:       true,
		CordonReason:   "maintenance",
		CordonedBy:     "alice",
	}

	tests := map[string]struct {
		setup        func(repo *schedulermocks.MockExecutorRepository)
		name         string
		expected     []*schedulerobjects.ExecutorSummary
		expectedCode codes.Code
	}{
		"all executors sorted by name": {
			setup: func(repo *schedulermocks.MockExecutorRepository) {
				repo.EXPECT().GetExecutors(gomock.Any()).Return([]*internalschedulerobjects.Executor{internalExecutorB, internalExecutorA}, nil)
				repo.EXPECT().GetExecutorSettings(gomock.Any()).Return([]*internalschedulerobjects.ExecutorSettings{settingsB, settingsA, settingsGone}, nil)
			},
			expected: []*schedulerobjects.ExecutorSummary{executorA, executorB},
		},
		"single executor": {
			setup: func(repo *schedulermocks.MockExecutorRepository) {
				repo.EXPECT().GetExecutor(gomock.Any(), "executor-b").Return(internalExecutorB, nil)
				repo.EXPECT().GetExecutorSettingsByID(gomock.Any(), "executor-b").Return(settingsB, nil)
			},
			name:     "executor-b",
			expected: []*schedulerobjects.ExecutorSummary{executorB},
		},
		"single uncordoned executor": {
			setup: func(repo *schedulermocks.MockExecutorRepository) {
				repo.EXPECT().GetExecutor(gomock.Any(), "executor-a").Return(internalExecutorA, nil)
				repo.EXPECT().GetExecutorSettingsByID(gomock.Any(), "executor-a").Return(settingsA, nil)
			},
			name:     "executor-a",
			expected: []*schedulerobjects.ExecutorSummary{executorA},
		},
		"single executor without settings": {
			setup: func(repo *schedulermocks.MockExecutorRepository) {
				repo.EXPECT().GetExecutor(gomock.Any(), "executor-a").Return(internalExecutorA, nil)
				repo.EXPECT().GetExecutorSettingsByID(gomock.Any(), "executor-a").Return(nil, nil)
			},
			name:     "executor-a",
			expected: []*schedulerobjects.ExecutorSummary{executorA},
		},
		"name is not trimmed": {
			setup: func(repo *schedulermocks.MockExecutorRepository) {
				repo.EXPECT().GetExecutor(gomock.Any(), " executor-b ").Return(nil, nil)
			},
			name:         " executor-b ",
			expectedCode: codes.NotFound,
		},
		"unknown executor": {
			setup: func(repo *schedulermocks.MockExecutorRepository) {
				repo.EXPECT().GetExecutor(gomock.Any(), "executor-gone").Return(nil, nil)
			},
			name:         "executor-gone",
			expectedCode: codes.NotFound,
		},
		"no executors": {
			setup: func(repo *schedulermocks.MockExecutorRepository) {
				repo.EXPECT().GetExecutors(gomock.Any()).Return(nil, nil)
				repo.EXPECT().GetExecutorSettings(gomock.Any()).Return(nil, nil)
			},
			expected: []*schedulerobjects.ExecutorSummary{},
		},
		"executor lookup fails": {
			setup: func(repo *schedulermocks.MockExecutorRepository) {
				repo.EXPECT().GetExecutors(gomock.Any()).Return(nil, fmt.Errorf("db down"))
			},
			expectedCode: codes.Unavailable,
		},
		"settings lookup fails": {
			setup: func(repo *schedulermocks.MockExecutorRepository) {
				repo.EXPECT().GetExecutors(gomock.Any()).Return(nil, nil)
				repo.EXPECT().GetExecutorSettings(gomock.Any()).Return(nil, fmt.Errorf("db down"))
			},
			expectedCode: codes.Unavailable,
		},
		"single executor lookup fails": {
			setup: func(repo *schedulermocks.MockExecutorRepository) {
				repo.EXPECT().GetExecutor(gomock.Any(), "executor-a").Return(nil, fmt.Errorf("db down"))
			},
			name:         "executor-a",
			expectedCode: codes.Unavailable,
		},
		"single executor settings lookup fails": {
			setup: func(repo *schedulermocks.MockExecutorRepository) {
				repo.EXPECT().GetExecutor(gomock.Any(), "executor-a").Return(internalExecutorA, nil)
				repo.EXPECT().GetExecutorSettingsByID(gomock.Any(), "executor-a").Return(nil, fmt.Errorf("db down"))
			},
			name:         "executor-a",
			expectedCode: codes.Unavailable,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			defer cancel()

			repo := schedulermocks.NewMockExecutorRepository(gomock.NewController(t))
			tc.setup(repo)
			sut := NewServer(NewSchedulingContextRepository(), repo)
			resp, err := sut.GetExecutors(ctx, &schedulerobjects.ExecutorsRequest{Name: tc.name})

			if tc.expectedCode != codes.OK {
				require.Error(t, err)
				assert.Equal(t, tc.expectedCode, status.Code(err))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, resp.Executors)
		})
	}
}

// The REST gateway must write the heartbeat as a date-time string, as the OpenAPI schema says.
func TestServer_GetExecutors_Http(t *testing.T) {
	heartbeat := time.Date(2026, 11, 24, 10, 0, 0, 500, time.UTC)
	repo := schedulermocks.NewMockExecutorRepository(gomock.NewController(t))
	repo.EXPECT().GetExecutors(gomock.Any()).Return([]*internalschedulerobjects.Executor{
		{Id: "executor-a", Pool: "cpu", LastUpdateTime: protoutil.ToTimestamp(heartbeat)},
		{Id: "executor-b", Pool: "gpu"},
	}, nil)
	repo.EXPECT().GetExecutorSettings(gomock.Any()).Return(nil, nil)
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

	// Go clients read the response into the generated type.
	var decoded schedulerobjects.ExecutorsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &decoded))
	require.Len(t, decoded.Executors, 2)
	assert.Equal(t, "executor-a", decoded.Executors[0].Name)
	assert.True(t, time.Unix(decoded.Executors[0].LastUpdateTime.Seconds, int64(decoded.Executors[0].LastUpdateTime.Nanos)).Equal(heartbeat))
	assert.Nil(t, decoded.Executors[1].LastUpdateTime)
}
