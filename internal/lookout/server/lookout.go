package server

import (
	"context"
	"fmt"

	"github.com/gogo/protobuf/types"

	"github.com/G-Research/armada/pkg/api/lookout"
)

type LookoutServer struct {
}

func (s *LookoutServer) Overview(ctx context.Context, _ *types.Empty) (*lookout.SystemOverview, error) {
	return nil, fmt.Errorf("implement me")
}
