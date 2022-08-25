package eventscheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/severinson/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/common/eventutil/eventid"
	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/internal/common/requestid"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/internal/pulsarutils/pulsarrequestid"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// Service responsible for writing records derived from pulsar messages into postgres.
//
// At a high level, the ingester:
// 1. Reads messages from pulsar, which are used to create records.
// 2. Records are collected for up to some amount of time or records.
// 3. Records are batch-upserted into postgres.
// 4. The pulsar messages read to produce the batch are acked.
//
// The ingester handles the following messages:
//   - SubmitJob and JobRunLeased.
//     Creates new records in the jobs and runs table, respectively.
//   - ReprioritiseJob and ReprioritiseJobSet.
//     Updates the priority column for these jobs in-place. Does not mutate the job spec.
//   - CancelJob, CancelJobSet, JobSucceeded, and JobErrors.
//     Updates the state column for these jobs in-place.
//     Non-terminal JobErrors messages are ignored.
//   - JobRunAssigned.
//     Updates the runs table in-place with info of where the job is running.
//   - JobRunRunning, JobRunSucceeded, and JobRunErrors.
//     Updates the state column for these runs in-place.
//     Non-terminal JobRunErrors messages are ignored.
//
// TODO: What do we do about ReprioritisedJob and CancelledJob?
//
// Because we just store records, upserts are idempotent.
//
// Each ingester instance can only write into a single postgres table.
// I.e., to populate multiple tables with different record types (e.g., jobs and leases),
// a separate ingester instance is required for each record type.
type Ingester struct {
	// Used to setup a Pulsar consumer.
	PulsarClient    pulsar.Client
	ConsumerOptions pulsar.ConsumerOptions
	// Write to postgres at least this often (assuming there are records to write).
	MaxWriteInterval time.Duration
	// Write current batch to postgres if at least this many records have been written to it.
	MaxWriteRecords int
	// Connection to the postgres database used for persistence.
	Db *pgxpool.Pool
	// For each partition, store the id of the most recent message.
	// Used to detect unexpected seeks.
	lastMessageIdByPartition map[int32]pulsar.MessageID
	// Pulsar consumer on which to receive messages.
	consumer pulsar.Consumer
	// Optional logger.
	// If not provided, the default logrus logger is used.
	Logger *logrus.Entry
}

// Batch of changes to be written to postgres.
type Batch struct {
	// Time at which this batch was created.
	CreatedAt time.Time
	// Ids of messages processed to create this batch.
	// Note that a batch may contain several message ids but not changes to be written to postgres.
	// Since only a subset of messages result in changes to postgres.
	MessageIds []pulsar.MessageID
	// New jobs to be inserted.
	// Should always be inserted first.
	Jobs []Job
	// New job runs to be inserted.
	// Should be inserted after jobs.
	Runs []Run
	// Reprioritisations.
	// When writing to postgres, the priority that was last written to Pulsar for each job wins out.
	// For ReprioritiseJobSet, the priority
	Reprioritisations []*ReprioritisationBatch
	// Set of job sets to be canceled.
	// The map is used as a set, i.e., the value of the bool doesn't matter.
	JobSetsCancelled map[string]bool
	// Set of jobs to be canceled.
	// The map is used as a set, i.e., the value of the bool doesn't matter.
	JobsCancelled map[uuid.UUID]bool
	// Set of jobs that have succeeded.
	JobsSucceeded map[uuid.UUID]bool
	// Any job error messages received.
	JobErrors []JobError
	// Map from run id to a struct describing the set of physical resources assigned to that run.
	JobRunAssignments map[uuid.UUID]*JobRunAssignment
	// Ids of job runs that have started running.
	JobRunsRunning map[uuid.UUID]bool
	// Ids of job runs that have succeeded.
	JobRunsSucceeded map[uuid.UUID]bool
	// Any job run error messages received.
	JobRunErrors []JobRunError
}

func NewBatch() *Batch {
	return &Batch{
		CreatedAt:         time.Now(),
		JobSetsCancelled:  make(map[string]bool),
		JobsCancelled:     make(map[uuid.UUID]bool),
		JobsSucceeded:     make(map[uuid.UUID]bool),
		JobRunAssignments: make(map[uuid.UUID]*JobRunAssignment),
		JobRunsRunning:    make(map[uuid.UUID]bool),
		JobRunsSucceeded:  make(map[uuid.UUID]bool),
	}
}

// Batch of reprioritisations.
// PrioritiesByJobSet should always be applied before PrioritiesByJob.
// ReprioritisationBatch
// To ensure changes are applied in the correct order.
type ReprioritisationBatch struct {
	PrioritiesByJobSet map[string]int64
	PrioritiesByJob    map[uuid.UUID]int64
}

func NewReprioritisationBatch() *ReprioritisationBatch {
	return &ReprioritisationBatch{
		PrioritiesByJobSet: make(map[string]int64),
		PrioritiesByJob:    make(map[uuid.UUID]int64),
	}
}

// ErrStaleBatch indicates that some (or all) records in batch are derivded from partitions
// for which the data stored in postgres is more recent.
type ErrStaleBatch struct {
	Batch         *Batch
	ErrStaleWrite *ErrStaleWrite
}

func (err *ErrStaleBatch) Error() string {
	return fmt.Sprintf("stale write for batch %+v: %s", err.Batch, err.ErrStaleWrite)
}

// ShouldWrite returns true if this batch should be written into postgres.
// TODO: Update
func (srv *Ingester) ShouldWrite(batch *Batch) bool {
	if batch == nil {
		return false
	}
	if time.Since(batch.CreatedAt) > srv.MaxWriteInterval {
		return true
	}
	if len(batch.MessageIds) > srv.MaxWriteRecords {
		return true
	}
	return false
}

// Run the ingester until experiencing an unrecoverable error.
func (srv *Ingester) Run(ctx context.Context) error {

	// Get the configured logger, or the standard logger if none is provided.
	var log *logrus.Entry
	if srv.Logger != nil {
		log = srv.Logger.WithField("service", "SchedulerIngester")
	} else {
		log = logrus.StandardLogger().WithField("service", "SchedulerIngester")
	}

	log.Info("service started")

	// Create a processing pipeline
	// receive from pulsar -> batch messages -> write to postgres and ack messages.
	for {

		// Scheduler ingester
		consumer, err := srv.PulsarClient.Subscribe(srv.ConsumerOptions)
		if err != nil {
			return errors.WithStack(err)
		}
		defer consumer.Close()
		srv.consumer = consumer

		// All services run within an errgroup.
		g, ctx := errgroup.WithContext(ctx)
		ctx = ctxlogrus.ToContext(ctx, log)

		// Receive Pulsar messages asynchronously.
		pulsarToChannel := pulsarutils.NewPulsarToChannel(srv.consumer)
		g.Go(func() error { return pulsarToChannel.Run(ctx) })

		// Batch messages for writing into postgres.
		// Batches are forwarded on batchChannel.
		batchChannel := make(chan *Batch)
		g.Go(func() error { return srv.ProcessMessages(ctx, pulsarToChannel.C, batchChannel) })

		// Write batches to postgres and, for each batch, ack all pulsar messages the batch was made up of.
		g.Go(func() error { return srv.ProcessBatches(ctx, batchChannel) })

		// Run pipeline until any error.
		err = g.Wait()
		// TODO: Detect recoverable errors.
		if err != nil {
			// Unrecoverable error; exit.
			return err
		}
	}
}

func (srv *Ingester) ProcessMessages(ctx context.Context, messageChannel <-chan pulsar.Message, batchChannel chan<- *Batch) error {
	log := ctxlogrus.Extract(ctx)

	// In-progress batches.
	batches := make([]*Batch, 0)

	// Ticker to trigger periodically writing to postgres.
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Run until ctx is canceled.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-messageChannel:

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

			// Any consumer can seek on any topic partition.
			// An unexpected seek may cause messages to be delivered out of order,
			// i.e., a received messages may not be newer than the previous message.
			//
			// We compare the id of each received message with that of the previous message
			// on the same partition to detect such out-of-order messages.
			//
			// Note that, while possible, this should be a rare occurrence.
			//
			// TODO: We should check for this and restart the consumer if it happens.
			partitionIdx := msg.ID().PartitionIdx()
			if srv.lastMessageIdByPartition == nil {
				srv.lastMessageIdByPartition = make(map[int32]pulsar.MessageID)
			}
			if lastMessageId, ok := srv.lastMessageIdByPartition[partitionIdx]; ok {
				msgIsOufOfOrder, err := pulsarutils.FromMessageId(lastMessageId).GreaterEqual(msg.ID())
				if err != nil {
					return err
				}
				if msgIsOufOfOrder {
					// pulsarutils.PulsarMessageId prints nicely, so we convert to those.
					return fmt.Errorf(
						"unexpected seek detected: received messages out of order for topic %s: id of received message is %s, but the previous message has id %s",
						msg.Topic(), pulsarutils.FromMessageId(msg.ID()), pulsarutils.FromMessageId(lastMessageId),
					)
				}
			}
			srv.lastMessageIdByPartition[partitionIdx] = msg.ID()

			var err error
			batches, err = srv.ProcessMessage(ctxWithLogger, msg, batches)
			if err != nil {
				logging.WithStacktrace(messageLogger, err).Warnf("processing message failed; ignoring")
			}

			// If we have more than 1 batch, apply all but the last immedtately.
			batchesToApply := make([]*Batch, 0)
			if len(batches) > 1 {
				batchesToApply = append(batchesToApply, batches[:len(batches)-1]...)
				batches = []*Batch{batches[len(batches)-1]}
			}

			// If the most recent batch is large enough,
			// apply it immedtately to avoid batches becoming too large.
			if len(batches) == 1 {
				batch := batches[len(batches)-1]
				if srv.ShouldWrite(batch) {
					batchesToApply = append(batchesToApply, batch)
					batches = make([]*Batch, 0)
				}
			}

			// Batches to be applied are forwarded to a separate goroutine responsible for writing to postgres.
			for _, batch := range batchesToApply {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case batchChannel <- batch:
				}
			}

		case <-ticker.C: // Periodically send batches to be applied to postgres.
			for _, batch := range batches {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case batchChannel <- batch:
				}
			}
			batches = make([]*Batch, 0)
		}
	}
}

func (srv *Ingester) ProcessMessage(ctx context.Context, msg pulsar.Message, batches []*Batch) ([]*Batch, error) {
	if msg == nil {
		return batches, nil
	}
	log := ctxlogrus.Extract(ctx)

	// Create a fresh batch if we don't already have one.
	if len(batches) == 0 {
		batches = append(batches, NewBatch())
	}
	batch := batches[len(batches)-1]

	// Store the message id in the batch.
	// So we can ack messages once they've been written to postgres.
	//
	// It's fine to write to postgres and then not ack, since writes are idempotent.
	// But it's not fine to ack messages not written to postgres,
	// since skipping messages isn't detected.
	//
	// We defer adding the message id to ensure it's added to the last batch created
	// from this sequence.
	defer func() {
		batch := batches[len(batches)-1]
		batch.MessageIds = append(batch.MessageIds, msg.ID())
	}()

	// Unmarshal and validate the message.
	sequence, err := eventutil.UnmarshalEventSequence(ctx, msg.Payload())
	if err != nil {
		return batches, err
	}
	if sequence == nil || len(sequence.Events) == 0 {
		return batches, nil
	}

	// Update the current batch.
	for i, event := range sequence.GetEvents() {
		switch e := event.Event.(type) {
		case *armadaevents.EventSequence_Event_SubmitJob:
			// If there are job set operations to be applied,
			// we can't add new jobs for any affected job sets to this batch.
			// Since jobs submitted after those operations should not be affected.
			// To that end, we create new batches as necessary here.
			if _, ok := batch.JobSetsCancelled[sequence.GetJobSetName()]; ok {
				batches = append(batches, NewBatch())
				batch = batches[len(batches)-1]
			}
			for _, reprioritisation := range batch.Reprioritisations {
				if _, ok := reprioritisation.PrioritiesByJobSet[sequence.GetJobSetName()]; ok {
					batches = append(batches, NewBatch())
					batch = batches[len(batches)-1]
					break
				}
			}
			batch.Jobs = append(batch.Jobs, Job{
				JobID:         armadaevents.UuidFromProtoUuid(e.SubmitJob.JobId),
				JobSet:        sequence.GetJobSetName(),
				UserID:        sequence.GetUserId(),
				Groups:        sequence.GetGroups(),
				Queue:         sequence.GetQueue(),
				Priority:      int64(e.SubmitJob.Priority),
				SubmitMessage: msg.Payload(),
			})
		case *armadaevents.EventSequence_Event_JobRunLeased:
			batch.Runs = append(batch.Runs, Run{
				RunID:    armadaevents.UuidFromProtoUuid(e.JobRunLeased.GetRunId()),
				JobID:    armadaevents.UuidFromProtoUuid(e.JobRunLeased.GetJobId()),
				Executor: e.JobRunLeased.GetExecutorId(),
			})
		case *armadaevents.EventSequence_Event_ReprioritiseJob:
			if len(batch.Reprioritisations) == 0 {
				batch.Reprioritisations = append(batch.Reprioritisations, NewReprioritisationBatch())
			}
			reprioritisation := batch.Reprioritisations[len(batch.Reprioritisations)-1]
			newPriority := int64(e.ReprioritiseJob.GetPriority())
			if priority, ok := reprioritisation.PrioritiesByJobSet[sequence.JobSetName]; ok && priority == newPriority {
				break // This operation is redundant.
			}
			jobId := armadaevents.UuidFromProtoUuid(e.ReprioritiseJob.GetJobId())
			reprioritisation.PrioritiesByJob[jobId] = newPriority
		case *armadaevents.EventSequence_Event_ReprioritiseJobSet:
			if len(batch.Reprioritisations) == 0 {
				batch.Reprioritisations = append(batch.Reprioritisations, NewReprioritisationBatch())
			}
			reprioritisation := batch.Reprioritisations[len(batch.Reprioritisations)-1]

			// To ensure the most priority last written to Pulsar is applied last,
			// and since we apply ReprioritiseJobSet before ReprioritiseJob messages for each ReprioritisationBatch,
			// we need to create a new ReprioritisationBatch if len(reprioritisation.PrioritiesByJob) != 0.
			if len(reprioritisation.PrioritiesByJob) != 0 {
				batch.Reprioritisations = append(batch.Reprioritisations, NewReprioritisationBatch())
				reprioritisation = batch.Reprioritisations[len(batch.Reprioritisations)-1]
			}

			newPriority := int64(e.ReprioritiseJobSet.GetPriority())
			reprioritisation.PrioritiesByJobSet[sequence.GetJobSetName()] = newPriority
		case *armadaevents.EventSequence_Event_CancelJobSet:
			batch.JobSetsCancelled[sequence.GetJobSetName()] = true
		case *armadaevents.EventSequence_Event_CancelJob:
			jobId := armadaevents.UuidFromProtoUuid(e.CancelJob.GetJobId())
			batch.JobsCancelled[jobId] = true
		case *armadaevents.EventSequence_Event_JobSucceeded:
			jobId := armadaevents.UuidFromProtoUuid(e.JobSucceeded.GetJobId())
			batch.JobsSucceeded[jobId] = true
		case *armadaevents.EventSequence_Event_JobErrors:
			eventId := eventid.New(msg.ID(), i).String()
			for j, jobError := range e.JobErrors.GetErrors() {
				bytes, err := proto.Marshal(jobError)
				if err != nil {
					err = errors.WithStack(err)
					logging.WithStacktrace(log, err).Error("failed to marshal JobError")
				}
				batch.JobErrors = append(batch.JobErrors, JobError{
					// To ensure inserts are idempotent,
					// we need to mark each row with a deterministic id.
					ID:       fmt.Sprintf("%s-%d", eventId, j),
					JobID:    armadaevents.UuidFromProtoUuid(e.JobErrors.GetJobId()),
					Error:    bytes,
					Terminal: jobError.GetTerminal(),
				})
			}
		case *armadaevents.EventSequence_Event_JobRunAssigned:
			runId := armadaevents.UuidFromProtoUuid(e.JobRunAssigned.GetRunId())
			bytes, err := proto.Marshal(e.JobRunAssigned)
			if err != nil {
				err = errors.WithStack(err)
				logging.WithStacktrace(log, err).Error("failed to marshal JobRunAssigned")
			}
			batch.JobRunAssignments[runId] = &JobRunAssignment{
				RunID:      runId,
				Assignment: bytes,
			}
		case *armadaevents.EventSequence_Event_JobRunRunning:
			jobId := armadaevents.UuidFromProtoUuid(e.JobRunRunning.GetJobId())
			batch.JobRunsRunning[jobId] = true
		case *armadaevents.EventSequence_Event_JobRunSucceeded:
			jobId := armadaevents.UuidFromProtoUuid(e.JobRunSucceeded.GetJobId())
			batch.JobRunsSucceeded[jobId] = true
		case *armadaevents.EventSequence_Event_JobRunErrors:
			eventId := eventid.New(msg.ID(), i).String()
			for j, jobRunError := range e.JobRunErrors.GetErrors() {
				bytes, err := proto.Marshal(jobRunError)
				if err != nil {
					err = errors.WithStack(err)
					logging.WithStacktrace(log, err).Error("failed to marshal JobRunError")
				}
				batch.JobRunErrors = append(batch.JobRunErrors, JobRunError{
					// To ensure inserts are idempotent,
					// we need to mark each row with a deterministic id.
					ID:       fmt.Sprintf("%s-%d", eventId, j),
					RunID:    armadaevents.UuidFromProtoUuid(e.JobRunErrors.GetRunId()),
					Error:    bytes,
					Terminal: jobRunError.GetTerminal(),
				})
			}
		}
	}
	return batches, nil
}

func (srv *Ingester) ProcessBatches(ctx context.Context, batchChannel <-chan *Batch) error {
	log := ctxlogrus.Extract(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch := <-batchChannel:

			err := srv.ProcessBatch(ctx, batch)
			if err != nil {
				return err // TODO: Keep retrying on transient errors.
			}
			log.Infof("wrote batch records into postgres")

			// Ack all messages that were used to create the batch.
			// Acking is only safe once data has been written to postgres.
			for _, messageId := range batch.MessageIds {
				err := srv.consumer.AckID(messageId)
				if err != nil {
					return errors.WithStack(err)
				}
			}
		}
	}
}

func (srv *Ingester) ProcessBatch(ctx context.Context, batch *Batch) error {
	log := ctxlogrus.Extract(ctx)
	queries := New(srv.Db)

	// Jobs
	records := make([]interface{}, len(batch.Jobs))
	for i, job := range batch.Jobs {
		records[i] = job
	}
	err := Upsert(ctx, srv.Db, "jobs", JobsSchema(), records)
	if err != nil {
		return err // TODO: Keep retrying on transient failures.
	}
	log.Infof("wrote %d jobs into postgres", len(records))

	// Job runs
	records = make([]interface{}, len(batch.Runs))
	for i, run := range batch.Runs {
		records[i] = run
	}
	err = Upsert(ctx, srv.Db, "runs", RunsSchema(), records)
	if err != nil {
		return err // TODO: Keep retrying on transient failures.
	}
	log.Infof("wrote %d jobs into postgres", len(records))

	// Reprioritisations
	for _, reprioritisation := range batch.Reprioritisations {
		for jobSet, priority := range reprioritisation.PrioritiesByJobSet {
			err := queries.UpdateJobPriorityByJobSet(ctx, UpdateJobPriorityByJobSetParams{
				JobSet:   jobSet,
				Priority: priority,
			})
			if err != nil {
				return errors.WithStack(err)
			}
		}

		// TODO: This will be slow if there's a large number of ids.
		// Could be addressed by using a separate table for priority + upsert.
		for jobId, priority := range reprioritisation.PrioritiesByJob {
			err := queries.UpdateJobPriorityById(ctx, UpdateJobPriorityByIdParams{
				JobID:    jobId,
				Priority: priority,
			})
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}

	// Job sets cancelled
	jobSets := make([]string, 0, len(batch.JobSetsCancelled))
	for jobSet, _ := range batch.JobSetsCancelled {
		jobSets = append(jobSets, jobSet)
	}
	if len(jobSets) > 0 {
		err := queries.MarkJobsCancelledBySets(ctx, jobSets)
		if err != nil {
			return errors.WithStack(err)
		}
		err = queries.MarkJobRunsCancelledBySets(ctx, jobSets)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Jobs cancelled
	if len(batch.JobsCancelled) > 0 {
		jobIds := idsFromMap(batch.JobsCancelled)
		err := queries.MarkJobsCancelledById(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
		err = queries.MarkJobRunsCancelledByJobId(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Jobs succeeded
	err = queries.MarkJobsSucceededById(ctx, idsFromMap(batch.JobsSucceeded))
	if err != nil {
		return errors.WithStack(err)
	}

	// Job errors
	records = make([]interface{}, len(batch.JobErrors))
	failedJobIds := make([]uuid.UUID, 0)
	for i, jobError := range batch.JobErrors {
		records[i] = jobError
		if jobError.Terminal {
			failedJobIds = append(failedJobIds, jobError.JobID)
		}
	}
	err = Upsert(ctx, srv.Db, "job_errors", JobErrorsSchema(), records)
	if err != nil {
		return err // TODO: Keep retrying on transient failures.
	}
	log.Infof("wrote %d errors into postgres", len(records))

	// For terminal errors, mark the corresponding job as failed.
	if len(failedJobIds) > 0 {
		queries.MarkJobsFailedById(ctx, failedJobIds)
	}

	// Job run assignments
	i := 0
	records = make([]interface{}, len(batch.JobRunAssignments))
	for v, _ := range batch.JobRunAssignments {
		records[i] = v
		i++
	}
	err = Upsert(ctx, srv.Db, "job_run_assignments", JobRunAssignmentSchema(), records)
	if err != nil {
		return err // TODO: Keep retrying on transient failures.
	}
	log.Infof("wrote %d assignments into postgres", len(records))

	// Job runs running
	if len(batch.JobRunsRunning) > 0 {
		err := queries.MarkJobRunsRunningById(ctx, idsFromMap(batch.JobRunsRunning))
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Job runs succeeded
	if len(batch.JobRunsSucceeded) > 0 {
		err := queries.MarkJobRunsSucceededById(ctx, idsFromMap(batch.JobRunsSucceeded))
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Job run errors
	records = make([]interface{}, len(batch.JobRunErrors))
	failedJobRunIds := make([]uuid.UUID, 0)
	for i, jobRunError := range batch.JobRunErrors {
		records[i] = jobRunError
		if jobRunError.Terminal {
			failedJobIds = append(failedJobRunIds, jobRunError.RunID)
		}
	}
	err = Upsert(ctx, srv.Db, "job_run_errors", JobRunErrorsSchema(), records)
	if err != nil {
		return err // TODO: Keep retrying on transient failures.
	}
	log.Infof("wrote %d run errors into postgres", len(records))

	// For terminal errors, mark the corresponding job as failed.
	if len(failedJobRunIds) > 0 {
		queries.MarkJobRunsFailedById(ctx, failedJobRunIds)
	}

	return nil
}

func idsFromMap(set map[uuid.UUID]bool) []uuid.UUID {
	ids := make([]uuid.UUID, len(set))
	i := 0
	for id := range set {
		ids[i] = id
		i++
	}
	return ids
}
