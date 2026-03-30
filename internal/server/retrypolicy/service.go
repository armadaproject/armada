package retrypolicy

import (
	"context"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/server/permissions"
	"github.com/armadaproject/armada/pkg/api"
)

type Server struct {
	repository RetryPolicyRepository
	authorizer auth.ActionAuthorizer
}

func NewServer(repository RetryPolicyRepository, authorizer auth.ActionAuthorizer) *Server {
	return &Server{
		repository: repository,
		authorizer: authorizer,
	}
}

func (s *Server) CreateRetryPolicy(grpcCtx context.Context, req *api.RetryPolicy) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := s.authorizer.AuthorizeAction(ctx, permissions.CreateRetryPolicy)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "error creating retry policy %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
	}

	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "retry policy name must not be empty")
	}

	err = s.repository.CreateRetryPolicy(ctx, req)
	var ea *ErrRetryPolicyAlreadyExists
	if errors.As(err, &ea) {
		return nil, status.Errorf(codes.AlreadyExists, "error creating retry policy: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error creating retry policy: %s", err)
	}

	return &types.Empty{}, nil
}

func (s *Server) UpdateRetryPolicy(grpcCtx context.Context, req *api.RetryPolicy) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := s.authorizer.AuthorizeAction(ctx, permissions.UpdateRetryPolicy)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "error updating retry policy %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
	}

	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "retry policy name must not be empty")
	}

	err = s.repository.UpdateRetryPolicy(ctx, req)
	var enf *ErrRetryPolicyNotFound
	if errors.As(err, &enf) {
		return nil, status.Errorf(codes.NotFound, "error: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error updating retry policy %q: %s", req.Name, err)
	}

	return &types.Empty{}, nil
}

func (s *Server) DeleteRetryPolicy(grpcCtx context.Context, req *api.RetryPolicyDeleteRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := s.authorizer.AuthorizeAction(ctx, permissions.DeleteRetryPolicy)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "error deleting retry policy %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
	}

	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "retry policy name must not be empty")
	}

	err = s.repository.DeleteRetryPolicy(ctx, req.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error deleting retry policy %s: %s", req.Name, err)
	}
	return &types.Empty{}, nil
}

// GetRetryPolicy returns a single retry policy by name.
// Read access is intentionally unauthenticated, consistent with GetQueue in queue_service.go.
func (s *Server) GetRetryPolicy(grpcCtx context.Context, req *api.RetryPolicyGetRequest) (*api.RetryPolicy, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)

	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "retry policy name must not be empty")
	}

	policy, err := s.repository.GetRetryPolicy(ctx, req.Name)
	var enf *ErrRetryPolicyNotFound
	if errors.As(err, &enf) {
		return nil, status.Errorf(codes.NotFound, "error: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error getting retry policy %q: %s", req.Name, err)
	}
	return policy, nil
}

// GetRetryPolicies returns all retry policies.
// Read access is intentionally unauthenticated, consistent with GetQueue in queue_service.go.
func (s *Server) GetRetryPolicies(grpcCtx context.Context, _ *api.RetryPolicyListRequest) (*api.RetryPolicyList, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	policies, err := s.repository.GetAllRetryPolicies(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error getting retry policies: %s", err)
	}
	return &api.RetryPolicyList{RetryPolicies: policies}, nil
}
