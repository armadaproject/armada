package retrypolicy

import (
	"context"
	"strings"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/auth/permission"
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

func (s *Server) authorize(ctx *armadacontext.Context, perm permission.Permission, verb, name string) error {
	err := s.authorizer.AuthorizeAction(ctx, perm)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return status.Errorf(codes.PermissionDenied, "error %s retry policy %s: %s", verb, name, ep)
	}
	if err != nil {
		return status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
	}
	return nil
}

func (s *Server) CreateRetryPolicy(grpcCtx context.Context, req *api.RetryPolicy) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	if err := s.authorize(ctx, permissions.CreateRetryPolicy, "creating", req.Name); err != nil {
		return nil, err
	}

	if err := ValidatePolicy(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid retry policy: %s", err)
	}

	err := s.repository.CreateRetryPolicy(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error creating retry policy: %s", err)
	}

	return &types.Empty{}, nil
}

func (s *Server) UpdateRetryPolicy(grpcCtx context.Context, req *api.RetryPolicy) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	if err := s.authorize(ctx, permissions.UpdateRetryPolicy, "updating", req.Name); err != nil {
		return nil, err
	}

	if err := ValidatePolicy(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid retry policy: %s", err)
	}

	err := s.repository.UpdateRetryPolicy(ctx, req)
	var enf *ErrRetryPolicyNotFound
	if errors.As(err, &enf) {
		return nil, status.Errorf(codes.NotFound, "error: %s", err)
	}
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error updating retry policy %q: %s", req.Name, err)
	}

	return &types.Empty{}, nil
}

func (s *Server) DeleteRetryPolicy(grpcCtx context.Context, req *api.RetryPolicyDeleteRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	if err := s.authorize(ctx, permissions.DeleteRetryPolicy, "deleting", req.Name); err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "retry policy name must not be empty")
	}

	// Deleting always succeeds and detaches the policy from any queue using it,
	// so declarative tooling can delete resources in any order.
	detached, err := s.repository.DeleteRetryPolicy(ctx, req.Name)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error deleting retry policy %s: %s", req.Name, err)
	}
	if len(detached) > 0 {
		ctx.Infof("deleted retry policy %s and detached it from %d queue(s): %s", req.Name, len(detached), strings.Join(detached, ", "))
	}
	return &types.Empty{}, nil
}

// GetRetryPolicy returns a single retry policy by name.
// Reads require no permission, consistent with GetQueue.
func (s *Server) GetRetryPolicy(grpcCtx context.Context, req *api.RetryPolicyGetRequest) (*api.RetryPolicy, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)

	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "retry policy name must not be empty")
	}

	policy, err := s.repository.GetRetryPolicy(ctx, req.Name)
	var enf *ErrRetryPolicyNotFound
	if errors.As(err, &enf) {
		return nil, status.Errorf(codes.NotFound, "error: %s", err)
	}
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error getting retry policy %q: %s", req.Name, err)
	}
	return policy, nil
}

// GetRetryPolicies returns all retry policies.
// Reads require no permission, consistent with GetQueue.
func (s *Server) GetRetryPolicies(grpcCtx context.Context, _ *api.RetryPolicyListRequest) (*api.RetryPolicyList, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	policies, err := s.repository.GetAllRetryPolicies(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error getting retry policies: %s", err)
	}
	return &api.RetryPolicyList{RetryPolicies: policies}, nil
}
