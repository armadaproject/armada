package retrypolicy

import (
	"context"
	"fmt"
	"slices"
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
	"github.com/armadaproject/armada/pkg/client/queue"
)

// QueueLister supplies the queues checked before a retry policy is deleted.
type QueueLister interface {
	GetAllQueues(ctx *armadacontext.Context) ([]queue.Queue, error)
}

type Server struct {
	repository  RetryPolicyRepository
	queueLister QueueLister
	authorizer  auth.ActionAuthorizer
}

func NewServer(repository RetryPolicyRepository, queueLister QueueLister, authorizer auth.ActionAuthorizer) *Server {
	return &Server{
		repository:  repository,
		queueLister: queueLister,
		authorizer:  authorizer,
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
	var ea *ErrRetryPolicyAlreadyExists
	if errors.As(err, &ea) {
		return nil, status.Errorf(codes.AlreadyExists, "error creating retry policy: %s", err)
	}
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

	// Reject the delete while queues still reference the policy, so a queue is
	// never left pointing at a policy that no longer exists.
	//
	// This check and the delete below are not atomic: the queue and policy
	// stores are separate, so a queue could attach the policy in between. The
	// race is accepted because closing it would require a transaction spanning
	// both stores.
	referencing, err := s.queuesReferencingPolicy(ctx, req.Name)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error checking queues referencing retry policy %s: %s", req.Name, err)
	}
	if len(referencing) > 0 {
		shown := referencing[:min(len(referencing), maxReportedReferencingQueues)]
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"retry policy %s is still referenced by %d queue(s), including: %s",
			req.Name, len(referencing), strings.Join(shown, ", "),
		)
	}

	if err := s.repository.DeleteRetryPolicy(ctx, req.Name); err != nil {
		return nil, status.Errorf(codes.Unavailable, "error deleting retry policy %s: %s", req.Name, err)
	}
	return &types.Empty{}, nil
}

// maxReportedReferencingQueues caps the queue names listed in the delete error message.
const maxReportedReferencingQueues = 5

func (s *Server) queuesReferencingPolicy(ctx *armadacontext.Context, policyName string) ([]string, error) {
	queues, err := s.queueLister.GetAllQueues(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list queues: %w", err)
	}
	var names []string
	for _, q := range queues {
		if slices.Contains(q.RetryPolicies, policyName) {
			names = append(names, q.Name)
		}
	}
	return names, nil
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
