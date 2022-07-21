package eventscheduler

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/internal/common/requestid"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/internal/pulsarutils/pulsarrequestid"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
)

type SchedulerProcessor struct {
	Consumer pulsar.Consumer
	Producer pulsar.Producer
	// The scheduler tracks which executors are available.
	// True indicates the executor is ready to receive jobs.
	executors map[string]bool
	// Logger from which the loggers used by this service are derived
	// (e.g., using srv.Logger.WithField), or nil, in which case the global logrus logger is used.
	Logger *logrus.Entry
	mu     sync.Mutex
}

func NewSchedulerProcessor(consumer pulsar.Consumer, producer pulsar.Producer) *SchedulerProcessor {
	return &SchedulerProcessor{
		Consumer:  consumer,
		Producer:  producer,
		executors: make(map[string]bool),
	}
}

type ExecutorReport struct {
	Name string
	Time time.Time
}

// Schedule jobs by creating jobs leases for each new job.
func (srv *SchedulerProcessor) Run(ctx context.Context) error {

	// Get the configured logger, or the standard logger if none is provided.
	var log *logrus.Entry
	if srv.Logger != nil {
		log = srv.Logger.WithField("service", "SchedulerProcessor")
	} else {
		log = logrus.StandardLogger().WithField("service", "SchedulerProcessor")
	}
	log.Info("service started")

	// Receive Pulsar messages asynchronously.
	g, ctx := errgroup.WithContext(ctx)
	pulsarToChannel := pulsarutils.NewPulsarToChannel(srv.Consumer)
	g.Go(func() error { return pulsarToChannel.Run(ctx) })

	// Run until ctx is cancelled.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-pulsarToChannel.C:

			// Incoming gRPC requests are annotated with a unique id,
			// which is included with the corresponding Pulsar message.
			requestId := pulsarrequestid.FromMessageOrMissing(msg)

			// Put the requestId into a message-specific context and logger,
			// which are passed on to sub-functions.
			messageCtx, ok := requestid.AddToIncomingContext(ctx, requestId)
			if !ok {
				messageCtx = ctx
			}
			messageLogger := log.WithFields(logrus.Fields{"messageId": msg.ID(), requestid.MetadataKey: requestId})
			ctxWithLogger := ctxlogrus.ToContext(messageCtx, messageLogger)

			// Unmarshal and validate the message.
			sequence, err := eventutil.UnmarshalEventSequence(ctxWithLogger, msg.Payload())
			if err != nil {
				logging.WithStacktrace(messageLogger, err).Warnf("processing message failed; ignoring")
				srv.Consumer.Ack(msg)
				break
			}

			err = srv.ProcessSequence(ctx, requestId, sequence)
			if err != nil {
				logging.WithStacktrace(messageLogger, err).Warnf("processing message failed; ignoring")
			}
			srv.Consumer.Ack(msg)
		}
	}
}

func (srv *SchedulerProcessor) ProcessSequence(ctx context.Context, requestId string, sequence *armadaevents.EventSequence) error {
	log := ctxlogrus.Extract(ctx)

	// Spin until at least one executor becomes available.
	var candidateExecutors []string
	for len(candidateExecutors) == 0 {
		candidateExecutors = make([]string, 0)
		for executorName, isReady := range srv.executors {
			if isReady {
				candidateExecutors = append(candidateExecutors, executorName)
			}
		}
		if len(candidateExecutors) == 0 {
			log.Warnf("no executors available: %v", srv.executors)
			time.Sleep(5 * time.Second)
		}
	}

	sequenceToSend := &armadaevents.EventSequence{
		Queue:      sequence.Queue,
		JobSetName: sequence.JobSetName,
		UserId:     sequence.UserId,
		Groups:     sequence.Groups,
	}
	for _, event := range sequence.Events {
		if e := event.GetSubmitJob(); e != nil {
			created := time.Now()
			runId, err := armadaevents.ProtoUuidFromUlidString(util.NewULID())
			if err != nil {
				return err
			}

			// Filter out jobs not intended for this scheduler.
			if e.Scheduler != "pulsar" {
				continue
			}

			// Lease all jobs to the first executor.
			executor := candidateExecutors[0]
			sequenceToSend.Events = append(sequenceToSend.Events, &armadaevents.EventSequence_Event{
				Created: &created,
				Event: &armadaevents.EventSequence_Event_JobRunLeased{
					JobRunLeased: &armadaevents.JobRunLeased{
						RunId:      runId,
						JobId:      e.JobId,
						ExecutorId: executor,
					},
				},
			})
			log.Infof("leased job %s to executor %s", e.JobId, executor)
		}
	}

	if len(sequenceToSend.Events) == 0 {
		return nil
	}

	// Attempt to send forever.
	payload, err := proto.Marshal(sequenceToSend)
	if err != nil {
		return errors.WithStack(err)
	}
	for {
		_, err = srv.Producer.Send(
			ctx,
			&pulsar.ProducerMessage{
				Payload: payload,
				Properties: map[string]string{
					requestid.MetadataKey:                     requestId,
					armadaevents.PULSAR_MESSAGE_TYPE_PROPERTY: armadaevents.PULSAR_CONTROL_MESSAGE,
				},
				Key: sequence.JobSetName,
			},
		)
		if err != nil {
			logging.WithStacktrace(log, err).Warnf("sending message failed; retrying")
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}

	return nil
}

type SchedulerIngester struct {
	api.UnimplementedAggregatedQueueServer
	Consumer pulsar.Consumer
	Producer pulsar.Producer
	// Reference to the scheduler- to update the internal state of the scheduler.
	// Only ephemeral data (which executors are available) is written in this manner.
	Scheduler *SchedulerProcessor
	// Map from job id to the job.
	jobsById map[string]*api.Job
	// Map from executor name (id) to a map with the ids of jobs leased to that executor.
	// The bool indicates if the job has been given to the executor yet-
	// if true, the job has been sent to the executor.
	jobsByExecutor map[string]map[string]bool
	// Maps job lease ids to a bool indicating if the run is active.
	// Runs are active from their creation until the run succeeds or experiences a terminal error.
	activeByRun map[string]bool
	// Maps job ids to the name of the job set the job is associated with.
	jobSetByJobId map[string]string
	// Maps job ids to the queue the job is associated with.
	queueByJobId map[string]string
	// Map from jobId to the id of the currently active run of that job.
	// We ensure that only one run is active for each job at a time.
	runIdByJobId map[string]*armadaevents.Uuid
	// Logger from which the loggers used by this service are derived
	// (e.g., using srv.Logger.WithField), or nil, in which case the global logrus logger is used.
	Logger *logrus.Entry
	mu     sync.Mutex
}

func NewSchedulerIngester(consumer pulsar.Consumer, producer pulsar.Producer, scheduler *SchedulerProcessor) *SchedulerIngester {
	return &SchedulerIngester{
		Consumer:       consumer,
		Producer:       producer,
		Scheduler:      scheduler,
		jobsById:       make(map[string]*api.Job),
		jobsByExecutor: make(map[string]map[string]bool),
		activeByRun:    make(map[string]bool),
		jobSetByJobId:  make(map[string]string),
		queueByJobId:   make(map[string]string),
		runIdByJobId:   make(map[string]*armadaevents.Uuid),
	}
}

// Maintain the view of the scheduler state by reading from the log and writing to postgres.
func (srv *SchedulerIngester) Run(ctx context.Context) error {

	// Get the configured logger, or the standard logger if none is provided.
	var log *logrus.Entry
	if srv.Logger != nil {
		log = srv.Logger.WithField("service", "SchedulerIngester")
	} else {
		log = logrus.StandardLogger().WithField("service", "SchedulerIngester")
	}
	log.Info("service started")

	// Receive Pulsar messages asynchronously.
	g, ctx := errgroup.WithContext(ctx)
	pulsarToChannel := pulsarutils.NewPulsarToChannel(srv.Consumer)
	g.Go(func() error { return pulsarToChannel.Run(ctx) })

	// Run until ctx is cancelled.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-pulsarToChannel.C:
			srv.Consumer.Ack(msg)

			// Incoming gRPC requests are annotated with a unique id,
			// which is included with the corresponding Pulsar message.
			requestId := pulsarrequestid.FromMessageOrMissing(msg)

			// Put the requestId into a message-specific context and logger,
			// which are passed on to sub-functions.
			messageCtx, ok := requestid.AddToIncomingContext(ctx, requestId)
			if !ok {
				messageCtx = ctx
			}
			messageLogger := log.WithFields(logrus.Fields{"messageId": msg.ID(), requestid.MetadataKey: requestId})
			ctxWithLogger := ctxlogrus.ToContext(messageCtx, messageLogger)

			// Unmarshal and validate the message.
			sequence, err := eventutil.UnmarshalEventSequence(ctxWithLogger, msg.Payload())
			if err != nil {
				logging.WithStacktrace(messageLogger, err).Warnf("processing message failed; ignoring")
				break
			}

			err = srv.ProcessSequence(ctx, sequence)
			if err != nil {
				logging.WithStacktrace(messageLogger, err).Warnf("processing message failed; ignoring")
				break
			}
		}
	}
}

func (srv *SchedulerIngester) ProcessSequence(ctx context.Context, sequence *armadaevents.EventSequence) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	for _, event := range sequence.Events {
		if e := event.GetSubmitJob(); e != nil { // Store submitted jobs.

			// Filter out jobs not indented for this scheduler.
			if e.Scheduler != "pulsar" {
				continue
			}

			job, err := eventutil.ApiJobFromLogSubmitJob(sequence.UserId, sequence.Groups, sequence.Queue, sequence.JobSetName, time.Now(), e)
			if err != nil {
				return err
			}
			jobId := e.JobId.String()
			srv.jobsById[jobId] = job
			srv.jobSetByJobId[jobId] = sequence.JobSetName
		} else if e := event.GetJobRunLeased(); e != nil { // Record new leases.
			if m, ok := srv.jobsByExecutor[e.ExecutorId]; ok {
				m[e.JobId.String()] = false
			} else {
				m := make(map[string]bool)
				m[e.JobId.String()] = false
				srv.jobsByExecutor[e.ExecutorId] = m
			}
			srv.runIdByJobId[e.JobId.String()] = e.RunId
		} else if e := event.GetJobRunErrors(); e != nil { // Mark leases as inactive if failed.
			for _, v := range e.Errors {
				if v.Terminal {
					srv.activeByRun[e.RunId.String()] = false
				}
			}
		} else if e := event.GetJobRunSucceeded(); e != nil { // Mark leases as inactive if succeeded.
			srv.activeByRun[e.RunId.String()] = false
		}
	}
	return nil
}

func (srv *SchedulerIngester) LeaseJobs(ctx context.Context, req *api.LeaseRequest) (*api.JobLease, error) {

	// Register this executor as active
	srv.Scheduler.mu.Lock()
	srv.Scheduler.executors[req.ClusterId] = true
	srv.Scheduler.mu.Unlock()

	srv.mu.Lock()
	defer srv.mu.Unlock()
	jobLease := &api.JobLease{
		Job: make([]*api.Job, 0),
	}
	if jobIds, ok := srv.jobsByExecutor[req.ClusterId]; ok {
		for jobId, isSent := range jobIds {
			if !isSent {
				if job, ok := srv.jobsById[jobId]; ok {
					jobLease.Job = append(jobLease.Job, job)
				}

				// Mark the job as sent to avoid sending it again.
				jobIds[jobId] = true
			}
		}
	}
	return jobLease, nil
}

func (srv *SchedulerIngester) StreamingLeaseJobs(stream api.AggregatedQueue_StreamingLeaseJobsServer) error {

	log := ctxlogrus.Extract(stream.Context())
	srv.mu.Lock()
	defer srv.mu.Unlock()

	// Receive once to get info necessary to get jobs to lease.
	req, err := stream.Recv()
	if err != nil {
		return errors.WithStack(err)
	}

	// Register this executor as active
	srv.Scheduler.mu.Lock()
	if isReady, ok := srv.Scheduler.executors[req.ClusterId]; !isReady || !ok {
		log.Infof("executor %s becomes ready", req.ClusterId)
	}
	srv.Scheduler.executors[req.ClusterId] = true
	srv.Scheduler.mu.Unlock()

	// Collect all the jobs to be leased.
	jobsToLease := make([]*api.Job, 0)
	if jobIds, ok := srv.jobsByExecutor[req.ClusterId]; ok {
		for jobId, isSent := range jobIds {
			if !isSent {
				if job, ok := srv.jobsById[jobId]; ok {
					jobsToLease = append(jobsToLease, job)
				}
			}
		}
	}

	// The server streams jobs to the executor.
	// The executor streams back an ack for each received job.
	// With each job sent to the executor, the server includes the number of received acks.
	//
	// When the connection breaks, the server expires all leases for which it hasn't received an ack
	// and the executor expires all leases for which it hasn't received confirmation that the server received the ack.
	//
	// We track the total number of jobs and the number of jobs for which acks have been received.
	// Because gRPC streams guarantee ordering, we only need to track the number of acks.
	// The client is responsible for acking jobs in the order they are received.
	numJobs := uint32(len(jobsToLease))
	var numAcked uint32

	// Stream the jobs to the executor.
	g, _ := errgroup.WithContext(stream.Context())
	g.Go(func() error {
		for _, job := range jobsToLease {
			err := stream.Send(&api.StreamingJobLease{
				Job:      job,
				NumJobs:  numJobs,
				NumAcked: atomic.LoadUint32(&numAcked),
			})
			if err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
		}
		return nil
	})

	// Listen for job ids being streamed back as they're received.
	g.Go(func() error {
		numJobs := numJobs // Assign a local variable to guarantee there are no race conditions.
		for atomic.LoadUint32(&numAcked) < numJobs {
			ack, err := stream.Recv()
			if err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
			atomic.AddUint32(&numAcked, uint32(len(ack.ReceivedJobIds)))
			if m, ok := srv.jobsByExecutor[req.ClusterId]; ok {
				for _, jobId := range ack.ReceivedJobIds {
					m[jobId] = true // Mark job as sent.
				}
			}
		}
		return nil
	})

	// Wait for all jobs to have been sent and all acks to have been received.
	err = g.Wait()
	if err != nil {
		log.WithError(err).Error("error sending/receiving job leases to/from executor")
	}

	// Send one more message with the total number of acks.
	err = stream.Send(&api.StreamingJobLease{
		Job:      nil, // Omitted
		NumJobs:  numJobs,
		NumAcked: numAcked,
	})
	if err != nil {
		log.WithError(err).Error("error sending the number of acks")
	}

	return nil
}

func (srv *SchedulerIngester) RenewLease(ctx context.Context, req *api.RenewLeaseRequest) (*api.IdList, error) {
	log := ctxlogrus.Extract(ctx)
	log.Infof("executor %s renewed jobs %v", req.ClusterId, req.Ids)
	return &api.IdList{
		Ids: req.Ids,
	}, nil
}

func (srv *SchedulerIngester) ReturnLease(ctx context.Context, req *api.ReturnLeaseRequest) (*types.Empty, error) {
	log := ctxlogrus.Extract(ctx)
	log.Infof("executor %s returned %s", req.ClusterId, req.JobId)

	jobSetName, ok := srv.jobSetByJobId[req.JobId]
	if !ok {
		return &types.Empty{}, errors.Errorf("unknown job id %s: could not find job set name", req.JobId)
	}
	queue, ok := srv.queueByJobId[req.JobId]
	if !ok {
		return &types.Empty{}, errors.Errorf("unknown job id %s: could not find queue", req.JobId)
	}
	runId, ok := srv.runIdByJobId[req.JobId]
	if !ok {
		return &types.Empty{}, errors.Errorf("unknown job id %s: could not find run id", req.JobId)
	}
	sequence := &armadaevents.EventSequence{
		Queue:      queue,
		JobSetName: jobSetName,
	}

	jobId, err := armadaevents.ProtoUuidFromUuidString(req.JobId)
	if err != nil {
		return &types.Empty{}, err
	}

	sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				RunId: runId,
				JobId: jobId,
				Errors: []*armadaevents.Error{
					{
						Terminal: true, // EventMessage_LeaseReturned indicates a pod could not be scheduled.
						Reason: &armadaevents.Error_PodLeaseReturned{
							PodLeaseReturned: &armadaevents.PodLeaseReturned{
								ObjectMeta: &armadaevents.ObjectMeta{
									ExecutorId:   req.ClusterId,
									KubernetesId: "", // TODO: The fields explicitly set empty here should be set, but are not available in req.
								},
								PodNumber: 0,
								Message:   "",
							},
						},
					},
				},
			},
		},
	})

	sequences, err := eventutil.LimitSequenceByteSize(sequence, 4194304) // 4MB
	if err != nil {
		return &types.Empty{}, err
	}

	for _, sequence := range sequences {
		payload, err := proto.Marshal(sequence)
		if err != nil {
			return &types.Empty{}, errors.WithStack(err)
		}
		_, err = srv.Producer.Send(
			ctx,
			&pulsar.ProducerMessage{
				Payload: payload,
				Properties: map[string]string{
					requestid.MetadataKey:                     requestid.FromContextOrMissing(ctx),
					armadaevents.PULSAR_MESSAGE_TYPE_PROPERTY: armadaevents.PULSAR_CONTROL_MESSAGE,
				},
				Key: sequence.JobSetName,
			},
		)
		if err != nil {
			return &types.Empty{}, errors.WithStack(err)
		}
	}

	return &types.Empty{}, nil
}

func (srv *SchedulerIngester) ReportDone(ctx context.Context, req *api.IdList) (*api.IdList, error) {
	log := ctxlogrus.Extract(ctx)
	log.Infof("jobs %v reported done", req.Ids)

	sequences := make([]*armadaevents.EventSequence, 0)
	for _, jobId := range req.GetIds() {
		jobSetName, ok := srv.jobSetByJobId[jobId]
		if !ok {
			return nil, errors.Errorf("unknown job id %s: could not find job set name", jobId)
		}
		queue, ok := srv.queueByJobId[jobId]
		if !ok {
			return nil, errors.Errorf("unknown job id %s: could not find queue", jobId)
		}
		runId, ok := srv.runIdByJobId[jobId]
		if !ok {
			return nil, errors.Errorf("unknown job id %s: could not find run id", jobId)
		}
		sequence := &armadaevents.EventSequence{
			Queue:      queue,
			JobSetName: jobSetName,
		}

		jobId, err := armadaevents.ProtoUuidFromUuidString(jobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
				JobRunSucceeded: &armadaevents.JobRunSucceeded{
					RunId: runId,
					JobId: jobId,
				},
			},
		})
	}

	// Send the minimal number of sequences.
	sequences = eventutil.CompactEventSequences(sequences)
	sequences, err := eventutil.LimitSequencesByteSize(sequences, 4194304) // 4MB
	if err != nil {
		return nil, err
	}
	for _, sequence := range sequences {
		payload, err := proto.Marshal(sequence)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		_, err = srv.Producer.Send(
			ctx,
			&pulsar.ProducerMessage{
				Payload: payload,
				Properties: map[string]string{
					requestid.MetadataKey:                     requestid.FromContextOrMissing(ctx),
					armadaevents.PULSAR_MESSAGE_TYPE_PROPERTY: armadaevents.PULSAR_CONTROL_MESSAGE,
				},
				Key: sequence.JobSetName,
			},
		)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	return &api.IdList{
		Ids: req.Ids,
	}, nil
}

func (srv *SchedulerIngester) ReportUsage(ctx context.Context, req *api.ClusterUsageReport) (*types.Empty, error) {
	return &types.Empty{}, nil
}
