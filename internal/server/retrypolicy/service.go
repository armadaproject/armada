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
// Satisfied by both queue.PostgresQueueRepository and queue.CachedQueueRepository.
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

// authorize checks the caller holds perm, returning a gRPC status error
// (PermissionDenied for an unauthorized principal, Unavailable if the check
// itself fails) or nil when allowed. verb and name only shape the denial message.
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

	// Reject invalid policies up front. The scheduler validates policies when
	// loading them and silently drops bad ones, so without this check the
	// operator gets a 200 OK and only sees the policy fail to install later.
	if err := ValidatePolicy(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid retry policy: %s", err)
	}

	err := s.repository.CreateRetryPolicy(ctx, req)
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
	} else if err != nil {
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

	// Deleting a policy that queues still reference would leave those queues
	// pointing at nothing, silently disabling their retries. Force the
	// operator to detach the policy from all queues first.
	//
	// This check and the delete below are not atomic: the queue store and the
	// policy store are separate, so a queue could attach this policy in the
	// window between them. That race is left unguarded deliberately. The worst
	// outcome is a queue referencing a now-deleted policy, which the scheduler
	// treats as "no policy" and falls back to the default retry behaviour, so
	// it degrades gracefully rather than corrupting state. Closing it fully
	// would require a transaction spanning both stores.
	referencing, err := s.queuesReferencingPolicy(ctx, req.Name)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error checking queues referencing retry policy %s: %s", req.Name, err)
	}
	if len(referencing) > 0 {
		shown := referencing
		if len(shown) > maxReportedReferencingQueues {
			shown = shown[:maxReportedReferencingQueues]
		}
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"retry policy %s is still referenced by %d queue(s), including: %s",
			req.Name, len(referencing), strings.Join(shown, ", "),
		)
	}

	err = s.repository.DeleteRetryPolicy(ctx, req.Name)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error deleting retry policy %s: %s", req.Name, err)
	}
	return &types.Empty{}, nil
}

// Cap on queue names included in the FailedPrecondition message so a policy
// referenced by hundreds of queues does not produce an unreadable error.
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
// Read requires no permission, consistent with GetQueue; authentication still happens at the gRPC layer.
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
// Read requires no permission, consistent with GetQueue; authentication still happens at the gRPC layer.
func (s *Server) GetRetryPolicies(grpcCtx context.Context, _ *api.RetryPolicyListRequest) (*api.RetryPolicyList, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	policies, err := s.repository.GetAllRetryPolicies(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error getting retry policies: %s", err)
	}
	return &api.RetryPolicyList{RetryPolicies: policies}, nil
}
