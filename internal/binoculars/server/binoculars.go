package server

import (
	"context"
	"strconv"

	"github.com/armadaproject/armada/internal/binoculars/logs"
	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/pkg/api/binoculars"
)

type BinocularsServer struct {
	logService logs.LogService
}

func NewBinocularsServer(logService logs.LogService) *BinocularsServer {
	return &BinocularsServer{logService: logService}
}

func (b BinocularsServer) Logs(ctx context.Context, request *binoculars.LogRequest) (*binoculars.LogResponse, error) {
	principal := authorization.GetPrincipal(ctx)

	logLines, err := b.logService.GetLogs(ctx, &logs.LogParams{
		Principal:  principal,
		Namespace:  request.PodNamespace,
		PodName:    common.PodNamePrefix + request.JobId + "-" + strconv.Itoa(int(request.PodNumber)),
		SinceTime:  request.SinceTime,
		LogOptions: request.LogOptions,
	})
	if err != nil {
		return nil, err
	}

	return &binoculars.LogResponse{Log: logLines}, nil
}
