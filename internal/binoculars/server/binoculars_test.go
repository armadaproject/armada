package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/binoculars/service"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/api/binoculars"
)

func TestBinocularsServer_Logs_ReadsThePodOfTheRequest(t *testing.T) {
	tests := map[string]struct {
		request     *binoculars.LogRequest
		wantPodName string
	}{
		"a request with a pod name reads that pod": {
			request:     &binoculars.LogRequest{JobId: "job-1", PodName: "armada-run-1", PodNamespace: "team"},
			wantPodName: "armada-run-1",
		},
		"a request without a pod name reads the job-scoped pod": {
			request:     &binoculars.LogRequest{JobId: "job-1", PodNumber: 1, PodNamespace: "team"},
			wantPodName: "armada-job-1-1",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			logService := &recordingLogService{}
			server := NewBinocularsServer(logService, nil)

			_, err := server.Logs(context.Background(), tc.request)

			require.NoError(t, err)
			assert.Equal(t, tc.wantPodName, logService.params.PodName)
		})
	}
}

type recordingLogService struct {
	params *service.LogParams
}

func (s *recordingLogService) GetLogs(_ *armadacontext.Context, params *service.LogParams) ([]*binoculars.LogLine, error) {
	s.params = params
	return nil, nil
}
