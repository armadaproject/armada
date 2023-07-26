package main

import (
	"context"
	"fmt"
	"time"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/gogo/protobuf/types"
)

type PerformanceTestEventServer struct{}

func NewPerformanceTestEventServer() *PerformanceTestEventServer {
	return &PerformanceTestEventServer{}
}

func (s *PerformanceTestEventServer) Report(ctx context.Context, message *api.EventMessage) (*types.Empty, error) {
	return &types.Empty{}, nil
}

func (s *PerformanceTestEventServer) ReportMultiple(ctx context.Context, message *api.EventList) (*types.Empty, error) {
	return &types.Empty{}, nil
}

// GetJobSetEvents streams back all events associated with a particular job set.
func (s *PerformanceTestEventServer) GetJobSetEvents(request *api.JobSetRequest, stream api.Event_GetJobSetEventsServer) error {
	// FIXME: Handle case where watch is not True.
	return s.serveSimulatedEvents(request, stream)
}

func (s *PerformanceTestEventServer) Health(ctx context.Context, cont_ *types.Empty) (*api.HealthCheckResponse, error) {
	return &api.HealthCheckResponse{Status: api.HealthCheckResponse_SERVING}, nil
}

func (s *PerformanceTestEventServer) Watch(req *api.WatchRequest, stream api.Event_WatchServer) error {
	request := &api.JobSetRequest{
		Id:             req.JobSetId,
		Watch:          true,
		FromMessageId:  req.FromId,
		Queue:          req.Queue,
		ErrorIfMissing: true,
		ForceLegacy:    req.ForceLegacy,
		ForceNew:       req.ForceNew,
	}
	return s.GetJobSetEvents(request, stream)
}

type scriptedMessage struct {
	Delay       time.Duration
	MessageFunc func(*api.JobSetRequest) *api.EventMessage
}

var messageScript = []*scriptedMessage{
	{ // Submitted
		Delay: time.Duration(1),
		MessageFunc: func(request *api.JobSetRequest) *api.EventMessage {
			return &api.EventMessage{
				Events: &api.EventMessage_Submitted{
					Submitted: &api.JobSubmittedEvent{
						JobId:    "fake_job_id",
						JobSetId: request.Id,
						Queue:    request.Queue,
						Created:  time.Now(),
						Job: api.Job{
							Id:        "fake_job_id",
							ClientId:  "",
							Queue:     request.Queue,
							JobSetId:  request.Id,
							Namespace: "fakeNamespace",
							Created:   time.Now(),
						},
					},
				},
			}
		},
	},
	{ // Queued
		Delay: time.Duration(time.Second * 1),
		MessageFunc: func(request *api.JobSetRequest) *api.EventMessage {
			return &api.EventMessage{
				Events: &api.EventMessage_Queued{
					Queued: &api.JobQueuedEvent{
						JobId:    "fake_job_id",
						JobSetId: request.Id,
						Queue:    request.Queue,
						Created:  time.Now(),
					},
				},
			}
		},
	},
	{ // Running
		Delay: time.Duration(time.Second * 1),
		MessageFunc: func(request *api.JobSetRequest) *api.EventMessage {
			return &api.EventMessage{
				Events: &api.EventMessage_Running{
					Running: &api.JobRunningEvent{
						JobId:        "fake_job_id",
						JobSetId:     request.Id,
						Queue:        request.Queue,
						Created:      time.Now(),
						ClusterId:    "fakeCluster",
						KubernetesId: "fakeK8s",
						NodeName:     "fakeNode",
						PodNumber:    1,
						PodName:      "fakePod",
						PodNamespace: "fakeNamespace",
					},
				},
			}
		},
	},
	{ // Success
		Delay: time.Duration(time.Second * 10),
		MessageFunc: func(request *api.JobSetRequest) *api.EventMessage {
			return &api.EventMessage{
				Events: &api.EventMessage_Succeeded{
					Succeeded: &api.JobSucceededEvent{
						JobId:        "fake_job_id",
						JobSetId:     request.Id,
						Queue:        request.Queue,
						Created:      time.Now(),
						ClusterId:    "fakeCluster",
						KubernetesId: "fakeK8s",
						NodeName:     "fakeNode",
						PodNumber:    1,
						PodName:      "fakePod",
						PodNamespace: "fakeNamespace",
					},
				},
			}
		},
	},
}

func (s *PerformanceTestEventServer) serveSimulatedEvents(request *api.JobSetRequest, stream api.Event_GetJobSetEventsServer) error {
	nextId := 1

	for _, message := range messageScript {
		time.Sleep(message.Delay)
		err := stream.Send(&api.EventStreamMessage{
			Id:      fmt.Sprintf("%d", nextId),
			Message: message.MessageFunc(request),
		})
		if err != nil {
			return err
		}
		nextId += 1
	}

	// Keep the stream active but don't send anything
	time.Sleep(time.Minute * 10)

	return nil
}
