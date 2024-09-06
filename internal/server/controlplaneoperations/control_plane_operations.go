package controlplaneoperations

import (
	"context"
	"fmt"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	pulsarutils "github.com/armadaproject/armada/internal/common/pulsarutils/controlplaneevents"
	"github.com/armadaproject/armada/internal/server/permissions"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/controlplaneevents"
)

type Server struct {
	publisher  pulsarutils.Publisher
	authorizer auth.ActionAuthorizer
	clock      clock.Clock
}

func New(
	publisher pulsarutils.Publisher,
	authorizer auth.ActionAuthorizer,
) *Server {
	return &Server{
		publisher:  publisher,
		authorizer: authorizer,
		clock:      clock.RealClock{},
	}
}

func (s *Server) CordonExecutor(grpcCtx context.Context, req *api.ExecutorCordonRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := s.authorizer.AuthorizeAction(ctx, permissions.CordonExecutors)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "error cordoning executor %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
	}

	executorName := req.Name
	if executorName == "" {
		return nil, fmt.Errorf("cannot cordon executor with empty name")
	}

	es := &controlplaneevents.ControlPlaneEventV1{
		Created: protoutil.ToTimestamp(s.clock.Now().UTC()),
		Event: &controlplaneevents.ControlPlaneEventV1_CordonExecutor{
			CordonExecutor: &controlplaneevents.CordonExecutor{
				Name:   executorName,
				Reason: req.Reason,
			},
		},
	}

	err = s.publisher.PublishMessages(ctx, es)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to send events to Pulsar")
	}

	return &types.Empty{}, nil
}

func (s *Server) UncordonExecutor(grpcCtx context.Context, req *api.ExecutorUncordonRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := s.authorizer.AuthorizeAction(ctx, permissions.CordonExecutors)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "error uncordoning executor %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
	}

	executorName := req.Name
	if executorName == "" {
		return nil, fmt.Errorf("cannot uncordon executor with empty name")
	}

	es := &controlplaneevents.ControlPlaneEventV1{
		Created: protoutil.ToTimestamp(s.clock.Now().UTC()),
		Event: &controlplaneevents.ControlPlaneEventV1_UncordonExecutor{
			UncordonExecutor: &controlplaneevents.UncordonExecutor{
				Name:   executorName,
				Reason: req.Reason,
			},
		},
	}

	err = s.publisher.PublishMessages(ctx, es)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to send events to Pulsar")
	}

	return &types.Empty{}, nil
}
