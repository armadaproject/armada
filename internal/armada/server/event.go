package server

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/gogo/protobuf/types"
)

type EventServer struct {
}

func (EventServer) Report(context.Context, *api.EventMessage) (*types.Empty, error) {
	panic("implement me")
}

func (EventServer) GetJobSetEvents(*api.JobSetRequest, api.Event_GetJobSetEventsServer) error {

	panic("implement me")
}
