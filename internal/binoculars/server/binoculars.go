package server

import (
	"context"
	"strconv"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/armadaproject/armada/internal/binoculars/service"
	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/pkg/api/binoculars"
)

type BinocularsServer struct {
	logService    service.LogService
	cordonService service.CordonService
	binoculars.UnimplementedBinocularsServer
}

func NewBinocularsServer(logService service.LogService, cordonService service.CordonService) *BinocularsServer {
	return &BinocularsServer{
		logService:    logService,
		cordonService: cordonService,
	}
}

func (b *BinocularsServer) Logs(ctx context.Context, request *binoculars.LogRequest) (*binoculars.LogResponse, error) {
	principal := auth.GetPrincipal(ctx)

	logLines, err := b.logService.GetLogs(armadacontext.FromGrpcCtx(ctx), &service.LogParams{
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

func (b *BinocularsServer) Cordon(ctx context.Context, request *binoculars.CordonRequest) (*emptypb.Empty, error) {
	err := b.cordonService.CordonNode(armadacontext.FromGrpcCtx(ctx), request)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}
