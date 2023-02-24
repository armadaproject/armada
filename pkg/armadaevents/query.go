package armadaevents

import (
	"context"

	"github.com/armadaproject/armada/pkg/api"
)

// PulsarQueryServer responds to queue info queries based on the log messages.
type PulsarQueryServer struct {
	api.UnimplementedSubmitServer
}

func GetQueue(ctx context.Context, req *api.QueueGetRequest) (*api.Queue, error) {
	return &api.Queue{
		Name:           "",
		PriorityFactor: 0,
		UserOwners:     nil,
		GroupOwners:    nil,
		ResourceLimits: nil,
		Permissions:    nil,
	}, nil
}

func GetQueueInfo(ctx context.Context, req *api.QueueInfoRequest) (*api.QueueInfo, error) {
	return &api.QueueInfo{
		Name:          "",
		ActiveJobSets: nil,
	}, nil
}
