package node

import (
	"context"
	"errors"
	"fmt"

	"github.com/gogo/protobuf/types"
	"github.com/gogo/status"
	"google.golang.org/grpc/codes"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/server/permissions"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/controlplaneevents"
)

type Server struct {
	publisher  pulsarutils.Publisher[*controlplaneevents.Event]
	authorizer auth.ActionAuthorizer
	clock      clock.Clock
}

var _ api.NodeServer = (*Server)(nil)

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

// PreemptOnNode implements api.NodeServer.
func (s *Server) PreemptOnNode(grpcCtx context.Context, req *api.NodePreemptRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := s.authorizer.AuthorizeAction(ctx, permissions.PreemptAnyJobs)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "error preempting jobs on executor %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "error checking permissions: %s", err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("must provide non-empty executor name when determining what to preempt")
	}

	if req.Executor == "" {
		return nil, fmt.Errorf("must provide non-empty executor id when determining what to preempt")
	}

	es := &controlplaneevents.Event{
		Created: protoutil.ToTimestamp(s.clock.Now().UTC()),
		Event: &controlplaneevents.Event_PreemptOnNode{
			PreemptOnNode: &controlplaneevents.PreemptOnNode{
				Name:            req.Name,
				Executor:        req.Executor,
				Queues:          req.Queues,
				PriorityClasses: req.PriorityClasses,
			},
		},
	}

	if err := s.publisher.PublishMessages(ctx, es); err != nil {
		return nil, status.Errorf(codes.Internal, "error publishing preempt on node event for executor %s: %s", req.Name, err)
	}

	return &types.Empty{}, nil
}

// CancelOnNode implements api.NodeServer.
func (s *Server) CancelOnNode(grpcCtx context.Context, req *api.NodeCancelRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := s.authorizer.AuthorizeAction(ctx, permissions.CancelAnyJobs)

	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "error canceling jobs on executor %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "error checking permissions: %s", err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("must provide non-empty executor name when determining what to cancel")
	}

	if req.Executor == "" {
		return nil, fmt.Errorf("must provide non-empty executor id when determining what to cancel")
	}

	es := &controlplaneevents.Event{
		Created: protoutil.ToTimestamp(s.clock.Now().UTC()),
		Event: &controlplaneevents.Event_CancelOnNode{
			CancelOnNode: &controlplaneevents.CancelOnNode{
				Name:            req.Name,
				Executor:        req.Executor,
				Queues:          req.Queues,
				PriorityClasses: req.PriorityClasses,
			},
		},
	}

	if err := s.publisher.PublishMessages(ctx, es); err != nil {
		return nil, status.Errorf(codes.Internal, "error publishing cancel on node event for executor %s: %s", req.Name, err)
	}

	return &types.Empty{}, nil
}
