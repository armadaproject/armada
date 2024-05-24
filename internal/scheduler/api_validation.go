package scheduler

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/pkg/executorapi"
)

func validateLeaseRequest(ctx context.Context, req *executorapi.LeaseRequest) error {
	for _, node := range req.Nodes {
		for runId := range node.RunIdsByState {
			_, err := uuid.Parse(runId)
			if err != nil {
				return status.Error(codes.InvalidArgument, fmt.Sprintf("runIdsByState for node %s includes invalid run id '%s'", node.Name, runId))
			}
		}
	}

	return nil
}
