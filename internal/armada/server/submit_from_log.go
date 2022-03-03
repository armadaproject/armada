package server

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/common/requestid"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/events"
	"github.com/G-Research/armada/pkg/api"
)

// LogToArmada is a service that reads messages from Pulsar and updates the state of the Armada server accordingly.
// Calls into an embedded Armada submit server object.
type SubmitFromLog struct {
	SubmitServer *SubmitServer
	Consumer     pulsar.Consumer
	// Logger from which the loggers used by this service are derived
	// (e.g., using srv.Logger.WithField), or nil, in which case the global logrus logger is used.
	Logger *logrus.Logger
}

// Run the service that reads from Pulsar and updates Armada until the provided context is cancelled.
func (srv *SubmitFromLog) Run(ctx context.Context) {

	// Get the configured logger, or the standard logger if none is provided.
	log := srv.Logger
	if log == nil {
		log = logrus.StandardLogger()
	}
	log.Info("SubmitFromLog service started")
	defer log.Info("SubmitFromLog service stopped")

	// Periodically log the number of processed messages.
	logInterval := 10 * time.Second
	lastLogged := time.Now()
	numReceived := 0
	numErrored := 0
	var lastMessageId pulsar.MessageID
	lastMessageId = nil
	lastPublishTime := time.Now()

	// Run until ctx is cancelled.
	for {

		// Periodic logging.
		if time.Since(lastLogged) > logInterval {
			log.WithFields(
				logrus.Fields{
					"received":      numReceived,
					"succeeded":     numReceived - numErrored,
					"errored":       numErrored,
					"interval":      logInterval,
					"lastMessageId": lastMessageId,
					"timeLag":       time.Now().Sub(lastPublishTime),
				},
			).Info("message statistics")
			numReceived = 0
			numErrored = 0
			lastLogged = time.Now()
		}

		// Exit if the context has been cancelled. Otherwise, get a message from Pulsar.
		select {
		case <-ctx.Done():
			log.WithFields(
				logrus.Fields{
					"received":      numReceived,
					"succeeded":     numReceived - numErrored,
					"errored":       numErrored,
					"interval":      logInterval,
					"lastMessageId": lastMessageId,
					"timeLag":       time.Now().Sub(lastPublishTime),
				},
			).Info("shutdown signal received")
			break
		default:

			// Get a message from Pulsar, which consists of a sequence of events (i.e., state transitions).
			ctxWithTimeout, _ := context.WithTimeout(ctx, time.Second)
			msg, err := srv.Consumer.Receive(ctxWithTimeout)
			if errors.Is(err, context.DeadlineExceeded) {
				continue //expected
			}

			// If receiving fails, wait for a bit, since the problem may be transient.
			if err != nil {
				log.WithField("lastMessageId", lastMessageId).WithError(err).Warnf("Pulsar receive failed; backing off")
				time.Sleep(time.Second)
				continue
			}
			lastMessageId = msg.ID()
			lastPublishTime = msg.PublishTime()
			numReceived++

			// Incoming gRPC requests are annotated with a unique id,
			// which is included with the corresponding Pulsar message.
			requestId := "missing"
			if msg.Properties() != nil {
				if id, ok := msg.Properties()[requestid.MetadataKey]; ok {
					requestId = id
				}
			}

			// Put the requestId into a message-specific context and logger,
			// which are passed on to sub-functions.
			messageCtx, ok := requestid.AddToIncomingContext(ctx, requestId)
			if !ok {
				messageCtx = ctx
			}
			messageLogger := log.WithFields(logrus.Fields{"messageId": msg.ID(), requestid.MetadataKey: requestId})
			ctxWithLogger := ctxlogrus.ToContext(messageCtx, messageLogger)

			// Unmarshal and validate the message.
			sequence, err := srv.UnmarshalEventSequence(ctxWithLogger, msg.Payload())
			if err != nil {
				messageLogger.WithError(err).Warnf("processing message failed; ignoring")
				numErrored++
				continue
			}

			// Process the events in the sequence.
			// For efficiency, we may process several events at a time.
			// To maintain ordering, we only do so for subsequences of consecutive events with equal type.
			// TODO: Determine what to do based on the error (e.g., publish to a dead letter topic).
			i := 0
			for i < len(sequence.Events) {
				j, err := srv.ProcessSubSequence(ctxWithLogger, i, sequence)
				if err != nil {
					messageLogger.WithFields(logrus.Fields{"lowerIndex": i, "upperIndex": j}).WithError(err).Warnf("processing subsequence failed; ignoring")
					numErrored++
				}
				if j == i {
					j++ // Make sure we make progress in case of bugs in srv.ProcessSubSequence
				}
				i = j
			}
		}
	}
}

// UnmarshalEventSequence returns an EventSequence object contained in a byte buffer
// after validating that the resulting EventSequence is valid.
func (srv *SubmitFromLog) UnmarshalEventSequence(ctx context.Context, payload []byte) (*events.EventSequence, error) {
	sequence := &events.EventSequence{}
	err := proto.Unmarshal(payload, sequence)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}

	if sequence.JobSetName == "" {
		err = &armadaerrors.ErrInvalidArgument{
			Name:    "JobSetName",
			Value:   "",
			Message: "JobSetName not provided",
		}
		err = errors.WithStack(err)
		return nil, err
	}

	if sequence.UserId == "" {
		err = &armadaerrors.ErrInvalidArgument{
			Name:    "UserId",
			Value:   "",
			Message: "UserId not provided",
		}
		err = errors.WithStack(err)
		return nil, err
	}

	if sequence.Queue == "" {
		err = &armadaerrors.ErrInvalidArgument{
			Name:    "Queue",
			Value:   "",
			Message: "Queue name not provided",
		}
		err = errors.WithStack(err)
		return nil, err
	}

	if sequence.Groups == nil {
		sequence.Groups = make([]string, 0)
	}

	if sequence.Events == nil {
		err = &armadaerrors.ErrInvalidArgument{
			Name:    "Events",
			Value:   nil,
			Message: "no events in sequence",
		}
		err = errors.WithStack(err)
		return nil, err
	}
	return sequence, nil
}

// ProcessSubSequence processes sequence.Events[i:j-1], where j is the index of the first event in the sequence
// of a type different from that of sequence.Events[i], or len(sequence.Events) if no such event exists in the sequence,
// and returns j.
//
// Processing one such subsequence at a time preserves ordering between events of different types.
// For example, SubmitJob events are processed before CancelJob events that occur later in the sequence.
//
// Events are processed by calling into the embedded srv.SubmitServer.
func (srv *SubmitFromLog) ProcessSubSequence(ctx context.Context, i int, sequence *events.EventSequence) (j int, err error) {
	j = i + 1 // Return the next index to ensure that processing continues in case of unhandled errors at the caller
	if i < 0 || i >= len(sequence.Events) {
		err = &armadaerrors.ErrInvalidArgument{
			Name:    "i",
			Value:   i,
			Message: fmt.Sprintf("tried to index into a list composed of %d elements", len(sequence.Events)),
		}
		err = errors.WithStack(err)
		return
	}

	// Process the subsequence starting at the i-th event consisting of all consecutive events of the same type.
	switch sequence.Events[i].Event.(type) {
	case *events.EventSequence_Event_SubmitJob:
		es := collectJobSubmitEvents(ctx, i, sequence)
		err = srv.SubmitJobs(ctx, sequence.UserId, sequence.Groups, sequence.Queue, sequence.JobSetName, es)
		err = errors.WithStack(err)
		j = i + len(es)
	case *events.EventSequence_Event_CancelJob:
		es := collectCancelJobEvents(ctx, i, sequence)
		err = srv.CancelJobs(ctx, sequence.UserId, es)
		err = errors.WithStack(err)
		j = i + len(es)
	case *events.EventSequence_Event_CancelJobSet:
		es := collectCancelJobSetEvents(ctx, i, sequence)
		err = srv.CancelJobSets(ctx, sequence.Queue, sequence.JobSetName, es)
		err = errors.WithStack(err)
		j = i + len(es)
	case *events.EventSequence_Event_ReprioritiseJob:
		es := collectReprioritiseJobEvents(ctx, i, sequence)
		err = srv.ReprioritizeJobs(ctx, sequence.UserId, es)
		err = errors.WithStack(err)
		j = i + len(es)
	case *events.EventSequence_Event_ReprioritiseJobSet:
		es := collectReprioritiseJobSetEvents(ctx, i, sequence)
		err = srv.ReprioritizeJobSets(ctx, sequence.UserId, sequence.Queue, sequence.JobSetName, es)
		err = errors.WithStack(err)
		j = i + len(es)
		// Events not handled by this processor. Since the legacy scheduler writes these transitions directly to the db.
		//case *events.EventSequence_Event_JobSucceeded:
		//case *events.EventSequence_Event_JobFailed:
		//case *events.EventSequence_Event_JobRejected:
		//case *events.EventSequence_Event_JobRunLeased:
		//case *events.EventSequence_Event_JobRunAssigned:
		//case *events.EventSequence_Event_JobRunRunning:
		//case *events.EventSequence_Event_JobRunReturned:
		//case *events.EventSequence_Event_JobRunSucceeded:
		//case *events.EventSequence_Event_JobRunFailed:
	default:
		err = &armadaerrors.ErrInvalidArgument{
			Name:    fmt.Sprintf("Events[%d]", i),
			Value:   sequence.Events[i],
			Message: "received unsupported Pulsar event",
		}
		err = errors.WithStack(err)

		// Assign to j the index of the next event in the sequence with type different from sequence.Events[i],
		// or len(sequence.Events) if no such element exists, so that processing won't be attempted for this type again.
		j = i
		t := reflect.TypeOf(sequence.Events[i].Event)
		for j < len(sequence.Events) && reflect.TypeOf(sequence.Events[j].Event) == t {
			j++
		}
	}
	return
}

// collectJobSubmitEvents (and the corresponding functions for other types below)
// return a slice of events starting at index i in the sequence with equal type.
func collectJobSubmitEvents(ctx context.Context, i int, sequence *events.EventSequence) []*events.SubmitJob {
	result := make([]*events.SubmitJob, 0)
	for j := i; j < len(sequence.Events); j++ {
		if e, ok := sequence.Events[j].Event.(*events.EventSequence_Event_SubmitJob); ok {
			result = append(result, e.SubmitJob)
		} else {
			break
		}
	}
	return result
}

func collectCancelJobEvents(ctx context.Context, i int, sequence *events.EventSequence) []*events.CancelJob {
	result := make([]*events.CancelJob, 0)
	for j := i; j < len(sequence.Events); j++ {
		if e, ok := sequence.Events[j].Event.(*events.EventSequence_Event_CancelJob); ok {
			result = append(result, e.CancelJob)
		} else {
			break
		}
	}
	return result
}

func collectCancelJobSetEvents(ctx context.Context, i int, sequence *events.EventSequence) []*events.CancelJobSet {
	result := make([]*events.CancelJobSet, 0)
	for j := i; j < len(sequence.Events); j++ {
		if e, ok := sequence.Events[j].Event.(*events.EventSequence_Event_CancelJobSet); ok {
			result = append(result, e.CancelJobSet)
		} else {
			break
		}
	}
	return result
}

func collectReprioritiseJobEvents(ctx context.Context, i int, sequence *events.EventSequence) []*events.ReprioritiseJob {
	result := make([]*events.ReprioritiseJob, 0)
	for j := i; j < len(sequence.Events); j++ {
		if e, ok := sequence.Events[j].Event.(*events.EventSequence_Event_ReprioritiseJob); ok {
			result = append(result, e.ReprioritiseJob)
		} else {
			break
		}
	}
	return result
}

func collectReprioritiseJobSetEvents(ctx context.Context, i int, sequence *events.EventSequence) []*events.ReprioritiseJobSet {
	result := make([]*events.ReprioritiseJobSet, 0)
	for j := i; j < len(sequence.Events); j++ {
		if e, ok := sequence.Events[j].Event.(*events.EventSequence_Event_ReprioritiseJobSet); ok {
			result = append(result, e.ReprioritiseJobSet)
		} else {
			break
		}
	}
	return result
}

func (srv *SubmitFromLog) SubmitJobs(ctx context.Context, userId string, groups []string, queueName string, jobSetName string, es []*events.SubmitJob) error {

	jobs, err := apiJobsFromLogSubmitJobs(userId, groups, queueName, jobSetName, time.Now(), es)
	if err != nil {
		return err
	}

	// Submit the jobs by writing them to the database.
	submissionResults, err := srv.SubmitServer.jobRepository.AddJobs(jobs)
	if err != nil {
		jobFailures := createJobFailuresWithReason(jobs, fmt.Sprintf("Failed to save job in Armada: %s", err))
		reportErr := reportFailed(srv.SubmitServer.eventStore, "", jobFailures)
		if reportErr != nil {
			return status.Errorf(codes.Internal, "[SubmitJobs] error reporting failure event: %v", reportErr)
		}
		return status.Errorf(codes.Aborted, "[SubmitJobs] error saving jobs in Armada: %s", err)
	}

	// Create events that report what happened.
	// Later, this will be changed to submit log messages.
	var createdJobs []*api.Job
	var jobFailures []*jobFailure
	var doubleSubmits []*repository.SubmitJobResult
	for i, submissionResult := range submissionResults {
		jobResponse := &api.JobSubmitResponseItem{JobId: submissionResult.JobId}

		if submissionResult.Error != nil {
			jobResponse.Error = submissionResult.Error.Error()
			jobFailures = append(jobFailures, &jobFailure{
				job:    jobs[i],
				reason: fmt.Sprintf("Failed to save job in Armada: %s", submissionResult.Error.Error()),
			})
		} else if submissionResult.DuplicateDetected {
			doubleSubmits = append(doubleSubmits, submissionResult)
		} else {
			createdJobs = append(createdJobs, jobs[i])
		}
	}

	err = reportFailed(srv.SubmitServer.eventStore, "", jobFailures)
	if err != nil {
		return status.Errorf(codes.Internal, fmt.Sprintf("[SubmitJobs] error reporting failed jobs: %s", err))
	}

	err = reportDuplicateDetected(srv.SubmitServer.eventStore, doubleSubmits)
	if err != nil {
		return status.Errorf(codes.Internal, fmt.Sprintf("[SubmitJobs] error reporting duplicate jobs: %s", err))
	}

	err = reportQueued(srv.SubmitServer.eventStore, createdJobs)
	if err != nil {
		return status.Errorf(codes.Internal, fmt.Sprintf("[SubmitJobs] error reporting queued jobs: %s", err))
	}

	return nil
}

func apiJobsFromLogSubmitJobs(userId string, groups []string, queueName string, jobSetName string, time time.Time, es []*events.SubmitJob) ([]*api.Job, error) {
	jobs := make([]*api.Job, len(es), len(es))
	for i, e := range es {
		job, err := apiJobFromLogSubmitJob(userId, groups, queueName, jobSetName, time, e)
		if err != nil {
			return nil, err
		}
		jobs[i] = job
	}
	return jobs, nil
}

// apiJobFromLogSubmitJob converts a SubmitJob log message into an api.Job struct, which is used by Armada internally.
func apiJobFromLogSubmitJob(ownerId string, groups []string, queueName string, jobSetName string, time time.Time, e *events.SubmitJob) (*api.Job, error) {

	// The log submit API only supports a single pod spec. per job, which is placed in the main object.
	mainObject, ok := e.MainObject.Object.(*events.KubernetesMainObject_PodSpec)
	if !ok {
		return nil, errors.Errorf("expected *PodSpecWithAvoidList, but got %v", e.MainObject.Object)
	}
	podSpec := mainObject.PodSpec.PodSpec

	// The job submit message contains a bag of additional k8s objects to create as part of the job.
	// We need to separate those out into service and ingress types.
	k8sServices := make([]*v1.Service, 0)
	k8sIngresses := make([]*networking.Ingress, 0)
	k8sPodSpecs := make([]*v1.PodSpec, 0)
	k8sPodSpecs = append(k8sPodSpecs, podSpec)
	for _, object := range e.Objects {

		// The job-level ObjectMeta is the default.
		objectMeta := metav1.ObjectMeta{
			Namespace:   e.ObjectMeta.Namespace,
			Annotations: e.ObjectMeta.Annotations,
			Labels:      e.ObjectMeta.Labels,
		}

		// If a per-object ObjectMeta is provided, it takes precedence over the job-level one.
		if object.ObjectMeta != nil {
			objectMeta.Namespace = object.ObjectMeta.Namespace
			objectMeta.Annotations = object.ObjectMeta.Annotations
			objectMeta.Labels = object.ObjectMeta.Labels
		}
		switch o := object.Object.(type) {
		case *events.KubernetesObject_Service:
			k8sServices = append(k8sServices, &v1.Service{
				ObjectMeta: objectMeta,
				Spec:       *o.Service,
			})
		case *events.KubernetesObject_Ingress:
			k8sIngresses = append(k8sIngresses, &networking.Ingress{
				ObjectMeta: objectMeta,
				Spec:       *o.Ingress,
			})
		case *events.KubernetesObject_PodSpec:
			k8sPodSpecs = append(k8sPodSpecs, o.PodSpec.PodSpec)
		default:
			return nil, &armadaerrors.ErrInvalidArgument{
				Name:    "Objects",
				Value:   o,
				Message: "unsupported k8s object",
			}
		}
	}

	return &api.Job{
		Id:       e.JobId,
		ClientId: e.DeduplicationId,
		Queue:    queueName,
		JobSetId: jobSetName,

		Namespace:   e.ObjectMeta.Namespace,
		Labels:      e.ObjectMeta.Labels,
		Annotations: e.ObjectMeta.Annotations,

		K8SIngress: k8sIngresses,
		K8SService: k8sServices,

		Priority: e.Priority,

		PodSpecs:                 k8sPodSpecs,
		Created:                  time,
		Owner:                    ownerId,
		QueueOwnershipUserGroups: groups,
	}, nil
}

// CancelJobs cancels all jobs specified by the provided events in a single operation.
func (srv *SubmitFromLog) CancelJobs(ctx context.Context, userId string, es []*events.CancelJob) error {
	jobIds := make([]string, len(es), len(es))
	for i, e := range es {
		jobIds[i] = e.JobId
	}
	_, err := srv.CancelJobsById(ctx, userId, jobIds)
	return err
}

// CancelJobSets processes several CancelJobSet events.
// Because event sequences are specific to queue and job set, all CancelJobSet events in a sequence are equivalent,
// and we only need to call CancelJobSet once.
func (srv *SubmitFromLog) CancelJobSets(ctx context.Context, queueName string, jobSetName string, es []*events.CancelJobSet) error {
	return srv.CancelJobSet(ctx, queueName, jobSetName)
}

func (srv *SubmitFromLog) CancelJobSet(ctx context.Context, queueName string, jobSetName string) error {

	jobIds, err := srv.SubmitServer.jobRepository.GetActiveJobIds(queueName, jobSetName)
	if err != nil {
		return err
	}

	// Split IDs into batches and process one batch at a time.
	// To reduce the number of jobs stored in memory.
	jobIdBatches := util.Batch(jobIds, srv.SubmitServer.cancelJobsBatchSize)
	for _, jobIdBatch := range jobIdBatches {
		_, err := srv.CancelJobsById(ctx, queueName, jobIdBatch)
		if err != nil {
			return err
		}

		// TODO I think the right way to do this is to include a timeout with the call to Redis
		// Then, we can check for a deadline exceeded error here
		if util.CloseToDeadline(ctx, time.Second*1) {
			err = errors.Errorf("deadline exceeded")
			return errors.WithStack(err)
		}
	}

	return nil
}

// CancelJobsById cancels all jobs with the specified ids.
func (srv *SubmitFromLog) CancelJobsById(ctx context.Context, userId string, jobIds []string) ([]string, error) {

	jobs, err := srv.SubmitServer.jobRepository.GetExistingJobsByIds(jobIds)
	if err != nil {
		return nil, err
	}

	err = reportJobsCancelling(srv.SubmitServer.eventStore, userId, jobs)
	if err != nil {
		return nil, err
	}

	deletionResult, err := srv.SubmitServer.jobRepository.DeleteJobs(jobs)
	if err != nil {
		return nil, err
	}

	// Check which jobs cancelled successfully.
	// Collect any errors into a multierror.
	var result *multierror.Error
	cancelled := []*api.Job{}
	cancelledIds := []string{}
	for job, err := range deletionResult {
		if err != nil {
			result = multierror.Append(result, err)
		} else {
			cancelled = append(cancelled, job)
			cancelledIds = append(cancelledIds, job.Id)
		}
	}

	// Report the jobs that cancelled successfully.
	//Any error in doing so is a sibling to the errors with cancelling individual jobs.
	result = multierror.Append(result, reportJobsCancelled(srv.SubmitServer.eventStore, userId, cancelled))

	return cancelledIds, result.ErrorOrNil()
}

// ReprioritizeJobs updates the priority of one of more jobs.
func (srv *SubmitFromLog) ReprioritizeJobs(ctx context.Context, userId string, es []*events.ReprioritiseJob) error {
	if len(es) == 0 {
		return nil
	}

	jobIds := make([]string, len(es), len(es))
	for i, e := range es {
		jobIds[i] = e.JobId
	}
	jobs, err := srv.SubmitServer.jobRepository.GetExistingJobsByIds(jobIds)
	if err != nil {
		return err
	}

	// The submit API guarantees that all events specify the same priority.
	newPriority := es[0].Priority
	for _, e := range es {
		if e.Priority != newPriority {
			err = errors.Errorf("all ReprioritiseJob events must have the same priority")
			return errors.WithStack(err)
		}
	}

	err = reportJobsReprioritizing(srv.SubmitServer.eventStore, userId, jobs, newPriority)
	if err != nil {
		return err
	}

	_, err = srv.SubmitServer.reprioritizeJobs(jobIds, newPriority, userId)
	if err != nil {
		return err
	}

	return nil
}

func (srv *SubmitFromLog) ReprioritizeJobSets(ctx context.Context, userId string, queueName string, jobSetName string, es []*events.ReprioritiseJobSet) error {
	var result *multierror.Error
	for _, e := range es {
		result = multierror.Append(result, srv.ReprioritizeJobSet(ctx, userId, queueName, jobSetName, e))
	}
	return result.ErrorOrNil()
}

func (srv *SubmitFromLog) ReprioritizeJobSet(ctx context.Context, userId string, queueName string, jobSetName string, e *events.ReprioritiseJobSet) error {
	jobIds, err := srv.SubmitServer.jobRepository.GetActiveJobIds(queueName, jobSetName)
	if err != nil {
		return err
	}
	jobs, err := srv.SubmitServer.jobRepository.GetExistingJobsByIds(jobIds)
	if err != nil {
		return err
	}

	err = reportJobsReprioritizing(srv.SubmitServer.eventStore, userId, jobs, e.Priority)
	if err != nil {
		return err
	}

	_, err = srv.SubmitServer.reprioritizeJobs(jobIds, e.Priority, userId)
	if err != nil {
		return err
	}

	return nil
}
