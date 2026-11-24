package reports

import (
	"fmt"
	"testing"
	"time"

	"github.com/gogo/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	internalschedulerobjects "github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api/schedulerobjects"
)

type fakeExecutorRepository struct {
	executors   []*internalschedulerobjects.Executor
	settings    []*internalschedulerobjects.ExecutorSettings
	executorErr error
	settingsErr error
}

func (f *fakeExecutorRepository) GetExecutors(_ *armadacontext.Context) ([]*internalschedulerobjects.Executor, error) {
	return f.executors, f.executorErr
}

func (f *fakeExecutorRepository) GetExecutorSettings(_ *armadacontext.Context) ([]*internalschedulerobjects.ExecutorSettings, error) {
	return f.settings, f.settingsErr
}

func TestServer_GetExecutors(t *testing.T) {
	heartbeat := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	repo := &fakeExecutorRepository{
		executors: []*internalschedulerobjects.Executor{
			{
				Id:             "executor-b",
				Pool:           "gpu",
				LastUpdateTime: protoutil.ToTimestamp(heartbeat),
				Nodes:          []*internalschedulerobjects.Node{{Id: "node-1"}, {Id: "node-2"}},
			},
			{
				Id:             "executor-a",
				Pool:           "cpu",
				LastUpdateTime: protoutil.ToTimestamp(heartbeat),
			},
		},
		settings: []*internalschedulerobjects.ExecutorSettings{
			{ExecutorId: "executor-b", Cordoned: true, CordonReason: "maintenance", SetByUser: "alice"},
			// Settings can outlive the executor, these must not show up on their own.
			{ExecutorId: "executor-gone", Cordoned: true, CordonReason: "retired", SetByUser: "bob"},
		},
	}
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
		repo         *fakeExecutorRepository
		name         string
		expected     []*schedulerobjects.ExecutorSummary
		expectedCode codes.Code
	}{
		"all executors sorted by name": {
			repo:     repo,
			expected: []*schedulerobjects.ExecutorSummary{executorA, executorB},
		},
		"single executor": {
			repo:     repo,
			name:     " executor-b ",
			expected: []*schedulerobjects.ExecutorSummary{executorB},
		},
		"unknown executor": {
			repo:         repo,
			name:         "executor-gone",
			expectedCode: codes.NotFound,
		},
		"no executors": {
			repo:     &fakeExecutorRepository{},
			expected: []*schedulerobjects.ExecutorSummary{},
		},
		"executor lookup fails": {
			repo:         &fakeExecutorRepository{executorErr: fmt.Errorf("db down")},
			expectedCode: codes.Unavailable,
		},
		"settings lookup fails": {
			repo:         &fakeExecutorRepository{settingsErr: fmt.Errorf("db down")},
			expectedCode: codes.Unavailable,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			defer cancel()

			sut := NewServer(NewSchedulingContextRepository(), tc.repo)
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

func TestServer_GetExecutors_NoRepository(t *testing.T) {
	sut := NewServer(NewSchedulingContextRepository(), nil)
	_, err := sut.GetExecutors(armadacontext.Background(), &schedulerobjects.ExecutorsRequest{})
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}
