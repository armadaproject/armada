package server

import (
	"context"
	"strconv"

	"github.com/gogo/protobuf/types"

	"github.com/armadaproject/armada/internal/binoculars/service"
	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/pkg/api/binoculars"
)

type BinocularsServer struct {
	logService    service.LogService
	cordonService service.CordonService
}

func NewBinocularsServer(logService service.LogService, cordonService service.CordonService) *BinocularsServer {
	return &BinocularsServer{
		logService:    logService,
		cordonService: cordonService,
	}
}

func (b *BinocularsServer) Logs(ctx context.Context, request *binoculars.LogRequest) (*binoculars.LogResponse, error) {
	principal := authorization.GetPrincipal(ctx)

	logLines, err := b.logService.GetLogs(ctx, &service.LogParams{
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

func (b *BinocularsServer) Cordon(ctx context.Context, request *binoculars.CordonRequest) (*types.Empty, error) {
	err := b.cordonService.CordonNode(ctx, request)
	if err != nil {
		return nil, err
	}

	return &types.Empty{}, nil
}
