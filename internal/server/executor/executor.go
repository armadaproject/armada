package executor

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
	pulsarutils "github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/server/permissions"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/controlplaneevents"
)

type Server struct {
	publisher  pulsarutils.Publisher[*controlplaneevents.Event]
	authorizer auth.ActionAuthorizer
	clock      clock.Clock
}

func New(
	publisher pulsarutils.Publisher[*controlplaneevents.Event],
	authorizer auth.ActionAuthorizer,
) *Server {
	return &Server{
		publisher:  publisher,
		authorizer: authorizer,
		clock:      clock.RealClock{},
	}
}

func (s *Server) UpsertExecutorSettings(grpcCtx context.Context, req *api.ExecutorSettingsUpsertRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := s.authorizer.AuthorizeAction(ctx, permissions.UpdateExecutorSettings)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "error updating executor settings %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("must provide non-empty executor name when upserting executor settings")
	} else if req.Cordoned && req.CordonReason == "" {
		return nil, fmt.Errorf("cordon reason must be specified if cordoning")
	}

	es := &controlplaneevents.Event{
		Created: protoutil.ToTimestamp(s.clock.Now().UTC()),
		Event: &controlplaneevents.Event_ExecutorSettingsUpsert{
			ExecutorSettingsUpsert: &controlplaneevents.ExecutorSettingsUpsert{
				Name:         req.Name,
				Cordoned:     req.Cordoned,
				CordonReason: req.CordonReason,
			},
		},
	}

	err = s.publisher.PublishMessages(ctx, es)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to send events to Pulsar")
	}

	return &types.Empty{}, nil
}

func (s *Server) DeleteExecutorSettings(grpcCtx context.Context, req *api.ExecutorSettingsDeleteRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := s.authorizer.AuthorizeAction(ctx, permissions.UpdateExecutorSettings)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "error deleting executor settings %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("must provide non-empty executor name when deleting executor settings")
	}

	es := &controlplaneevents.Event{
		Created: protoutil.ToTimestamp(s.clock.Now().UTC()),
		Event: &controlplaneevents.Event_ExecutorSettingsDelete{
			ExecutorSettingsDelete: &controlplaneevents.ExecutorSettingsDelete{
				Name: req.Name,
			},
		},
	}

	err = s.publisher.PublishMessages(ctx, es)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to send events to Pulsar")
	}

	return &types.Empty{}, nil
}
