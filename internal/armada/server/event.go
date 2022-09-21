package server

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/G-Research/armada/internal/armada/repository/sequence"

	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client/queue"
)

type EventServer struct {
	permissions           authorization.PermissionChecker
	eventRepository       repository.EventRepository
	legacyEventRepository repository.EventRepository
	queueRepository       repository.QueueRepository
	jobRepository         repository.JobRepository
	eventStore            repository.EventStore
	defaultToLegacyEvents bool
}

func NewEventServer(
	permissions authorization.PermissionChecker,
	eventRepository repository.EventRepository,
	legacyEventRepository repository.EventRepository,
	eventStore repository.EventStore,
	queueRepository repository.QueueRepository,
	jobRepository repository.JobRepository,
	defaultToLegacyEvents bool,
) *EventServer {
	return &EventServer{
		permissions:           permissions,
		eventRepository:       eventRepository,
		legacyEventRepository: legacyEventRepository,
		eventStore:            eventStore,
		queueRepository:       queueRepository,
		jobRepository:         jobRepository,
		defaultToLegacyEvents: defaultToLegacyEvents,
	}
}

func (s *EventServer) Report(ctx context.Context, message *api.EventMessage) (*types.Empty, error) {
	if err := checkPermission(s.permissions, ctx, permissions.ExecuteJobs); err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "[Report] error: %s", err)
	}

	return &types.Empty{}, s.eventStore.ReportEvents([]*api.EventMessage{message})
}

func (s *EventServer) ReportMultiple(ctx context.Context, message *api.EventList) (*types.Empty, error) {
	if err := checkPermission(s.permissions, ctx, permissions.ExecuteJobs); err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "[ReportMultiple] error: %s", err)
	}

	if err := s.checkForPreemptedEvents(message); err != nil {
		return &types.Empty{}, err
	}

	return &types.Empty{}, s.eventStore.ReportEvents(message.Events)
}

func (s *EventServer) checkForPreemptedEvents(message *api.EventList) error {
	var preemptedEvents []*api.EventMessage_Preempted
	var jobIds []string

	for _, event := range message.Events {
		if event, ok := event.Events.(*api.EventMessage_Preempted); ok {
			preemptedEvents = append(preemptedEvents, event)
			if event.Preempted.JobId != "" {
				jobIds = append(jobIds, event.Preempted.JobId)
			}
			if event.Preempted.PreemptiveJobId != "" {
				jobIds = append(jobIds, event.Preempted.PreemptiveJobId)
			}
		}
	}

	if len(preemptedEvents) == 0 {
		return nil
	}

	jobs, err := s.jobRepository.GetJobsByIds(jobIds)
	if err != nil {
		return errors.WithMessage(err, "error fetching jobs for preempted and preemptive job ids")
	}
	jobInfos := make(map[string]*repository.JobResult, len(jobs))
	for _, job := range jobs {
		jobInfos[job.JobId] = job
	}
	for _, event := range preemptedEvents {
		if err := s.enrichPreemptedEvent(event, jobInfos); err != nil {
			return err
		}
	}

	return nil
}

func (s *EventServer) enrichPreemptedEvent(event *api.EventMessage_Preempted, jobInfos map[string]*repository.JobResult) error {
	if event.Preempted.JobId == "" {
		return errors.Errorf("invalid Preempted event: preempted job id is not set")
	}

	result, ok := jobInfos[event.Preempted.JobId]
	if !ok {
		return errors.Errorf("error fetching job for preempted pod job id %s: job does not exist", event.Preempted.JobId)
	}
	event.Preempted.JobSetId = result.Job.JobSetId
	event.Preempted.Queue = result.Job.Queue

	return nil
}

// GetJobSetEvents streams back all events associated with a particular job set.
func (s *EventServer) GetJobSetEvents(request *api.JobSetRequest, stream api.Event_GetJobSetEventsServer) error {
	q, err := s.queueRepository.GetQueue(request.Queue)
	var expected *repository.ErrQueueNotFound
	if errors.As(err, &expected) {
		return status.Errorf(codes.NotFound, "[GetJobSetEvents] Queue %s does not exist", request.Queue)
	} else if err != nil {
		return err
	}

	err = validateUserHasWatchPermissions(stream.Context(), s.permissions, q, request.Id)
	if err != nil {
		return status.Errorf(codes.PermissionDenied, "[GetJobSetEvents] %s", err)
	}

	eventRepository := s.determineEventRepository(request)

	return s.serveEventsFromRepository(request, eventRepository, stream)
}

func (s *EventServer) determineEventRepository(request *api.JobSetRequest) repository.EventRepository {
	// User has explicitly said they want to use the new event store
	if request.ForceNew {
		return s.eventRepository
	}

	// User has explicitly said they want to use the legacy event store
	if request.ForceLegacy {
		return s.legacyEventRepository
	}

	// It's not a valid new-style sequence number so we have to default to the legacy store
	if !sequence.IsValid(request.GetId()) {
		return s.legacyEventRepository
	}

	// Configuration says we should default to legacy store
	if s.defaultToLegacyEvents {
		return s.legacyEventRepository
	}

	return s.eventRepository
}

func (s *EventServer) Watch(req *api.WatchRequest, stream api.Event_WatchServer) error {
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

func (s *EventServer) serveEventsFromRepository(request *api.JobSetRequest, eventRepository repository.EventRepository,
	stream api.Event_GetJobSetEventsServer,
) error {
	if request.ErrorIfMissing {
		exists, err := eventRepository.CheckStreamExists(request.Queue, request.Id)
		if err != nil {
			return status.Errorf(codes.Unavailable, "[GetJobSetEvents] error when checking jobset exists: %s", err)
		}
		if !exists {
			return status.Errorf(codes.NotFound, "[GetJobSetEvents] Jobset %s for queue %s does not exist", request.Id, request.Queue)
		}
	}

	fromId := request.FromMessageId

	var timeout time.Duration = -1
	stopAfter := ""
	if request.Watch {
		timeout = 5 * time.Second
	} else {
		lastId, err := eventRepository.GetLastMessageId(request.Queue, request.Id)
		if err != nil {
			return status.Errorf(codes.Unavailable, "[GetJobSetEvents] error getting ID of last message: %s", err)
		}
		stopAfter = lastId
	}

	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}

		messages, err := eventRepository.ReadEvents(request.Queue, request.Id, fromId, 500, timeout)
		if err != nil {
			return status.Errorf(codes.Unavailable, "[GetJobSetEvents] error reading events: %s", err)
		}

		stop := len(messages) == 0
		for _, msg := range messages {
			fromId = msg.Id
			if fromId == stopAfter {
				stop = true
			}
			err = stream.Send(msg)
			if err != nil {
				return status.Errorf(codes.Unavailable, "[GetJobSetEvents] error sending event: %s", err)
			}
		}

		if !request.Watch && stop {
			return nil
		}
	}
}

func validateUserHasWatchPermissions(ctx context.Context, permsChecker authorization.PermissionChecker, q queue.Queue, jobSetId string) error {
	err := checkPermission(permsChecker, ctx, permissions.WatchAllEvents)
	var globalPermErr *ErrNoPermission
	if errors.As(err, &globalPermErr) {
		err = checkQueuePermission(permsChecker, ctx, q, permissions.WatchEvents, queue.PermissionVerbWatch)
		var queuePermErr *ErrNoPermission
		if errors.As(err, &queuePermErr) {
			return status.Errorf(codes.PermissionDenied, "error getting events for queue: %s, job set: %s: %s",
				q.Name, jobSetId, MergePermissionErrors(globalPermErr, queuePermErr))
		} else if err != nil {
			return status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
		}
	} else if err != nil {
		return status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
	}
	return nil
}
