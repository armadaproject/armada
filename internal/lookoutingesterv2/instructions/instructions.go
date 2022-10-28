package instructions

import (
	"context"
	"github.com/G-Research/armada/pkg/api"
	v1 "k8s.io/api/core/v1"
	"sort"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"k8s.io/utils/pointer"

	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/common/database"
	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/common/requestid"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/lookoutingesterv2/metrics"
	"github.com/G-Research/armada/internal/lookoutingesterv2/model"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/internal/pulsarutils/pulsarrequestid"
	"github.com/G-Research/armada/pkg/armadaevents"
)

type HasNodeName interface {
	GetNodeName() string
}

type Service struct {
	metrics *metrics.Metrics
}

type jobResources struct {
	Cpu              int64
	Memory           int64
	EphemeralStorage int64
	Gpu              int64
}

func New(m *metrics.Metrics) *Service {
	return &Service{metrics: m}
}

// Convert takes a channel containing incoming pulsar messages and returns a channel with the corresponding
// InstructionSets. Each pulsar message will generate exactly one InstructionSet.
func (s *Service) Convert(
	ctx context.Context,
	msgs chan *pulsarutils.ConsumerMessage,
	bufferSize int,
	userAnnotationPrefix string,
	compressor compress.Compressor,
) chan *model.InstructionSet {
	out := make(chan *model.InstructionSet, bufferSize)
	go func() {
		for msg := range msgs {
			instructions := s.ConvertMsg(ctx, msg, userAnnotationPrefix, compressor)
			out <- instructions
		}
		close(out)
	}()
	return out
}

// ConvertMsg converts a pulsar message into an InstructionSet.
// An instructionSet will always be produced even if errors are encountered via parsing.  In this case of errors, the
// resulting InstructionSet will contain all events that could be parsed, along with the mesageId of the original message.
// In the case that no events can be parsed (e.g. the message is not valid protobuf), an empty InstructionSet containing
// only the messageId will be returned.
func (s *Service) ConvertMsg(
	ctx context.Context,
	msg *pulsarutils.ConsumerMessage,
	userAnnotationPrefix string,
	compressor compress.Compressor,
) *model.InstructionSet {
	pulsarMsg := msg.Message

	ctxWithLogger, messageLogger := createCtxWithLogger(ctx, pulsarMsg)

	updateInstructions := &model.InstructionSet{
		MessageIds: []*pulsarutils.ConsumerMessageId{
			{pulsarMsg.ID(), 0, msg.ConsumerId},
		},
	}

	// It's not a control message - no instructions needed
	if !armadaevents.IsControlMessage(msg.Message) {
		return updateInstructions
	}

	// Try and unmarshal the proto - if it fails there's not much we can do here.
	sequence, err := eventutil.UnmarshalEventSequence(ctxWithLogger, pulsarMsg.Payload())
	if err != nil {
		s.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorDeserialization)
		messageLogger.Warnf("Could not unmarshall message %v", err)
		return updateInstructions
	}

	queue := sequence.Queue
	jobset := sequence.JobSetName
	owner := sequence.UserId
	ts := pulsarMsg.PublishTime()
	for idx, event := range sequence.Events {
		switch event.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_SubmitJob:
			err = s.handleSubmitJob(messageLogger, queue, owner, jobset, ts, event.GetSubmitJob(), userAnnotationPrefix, compressor, updateInstructions)
		case *armadaevents.EventSequence_Event_ReprioritisedJob:
			err = s.handleReprioritiseJob(ts, event.GetReprioritisedJob(), updateInstructions)
		case *armadaevents.EventSequence_Event_CancelledJob:
			err = s.handleCancelJob(ts, event.GetCancelledJob(), updateInstructions)
		case *armadaevents.EventSequence_Event_JobSucceeded:
			err = s.handleJobSucceeded(ts, event.GetJobSucceeded(), updateInstructions)
		case *armadaevents.EventSequence_Event_JobErrors:
			err = s.handleJobErrors(ts, event.GetJobErrors(), updateInstructions)
		case *armadaevents.EventSequence_Event_JobRunAssigned:
			err = s.handleJobRunAssigned(ts, event.GetJobRunAssigned(), updateInstructions)
		case *armadaevents.EventSequence_Event_JobRunRunning:
			err = s.handleJobRunRunning(ts, event.GetJobRunRunning(), updateInstructions)
		case *armadaevents.EventSequence_Event_JobRunSucceeded:
			err = s.handleJobRunSucceeded(ts, event.GetJobRunSucceeded(), updateInstructions)
		case *armadaevents.EventSequence_Event_JobRunErrors:
			err = s.handleJobRunErrors(ts, messageLogger, event.GetJobRunErrors(), updateInstructions, compressor)
		case *armadaevents.EventSequence_Event_JobDuplicateDetected:
			err = s.handleJobDuplicateDetected(ts, event.GetJobDuplicateDetected(), updateInstructions)
		case *armadaevents.EventSequence_Event_CancelJob:
		case *armadaevents.EventSequence_Event_JobRunLeased:
		case *armadaevents.EventSequence_Event_ReprioritiseJobSet:
		case *armadaevents.EventSequence_Event_CancelJobSet:
		case *armadaevents.EventSequence_Event_ResourceUtilisation:
		case *armadaevents.EventSequence_Event_StandaloneIngressInfo:
		case *armadaevents.EventSequence_Event_JobRunPreempted:
			messageLogger.Debugf("Ignoring event type %T", event)
		default:
			messageLogger.Warnf("Ignoring unknown event type %T", event)
		}
		if err != nil {
			s.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
			messageLogger.Warnf("Could not convert event at index %d. %+v", idx, err)
		}
	}
	return updateInstructions
}

func (s *Service) handleSubmitJob(
	logger logrus.FieldLogger,
	queue string,
	owner string,
	jobSet string,
	ts time.Time,
	event *armadaevents.SubmitJob,
	userAnnotationPrefix string,
	compressor compress.Compressor,
	update *model.InstructionSet,
) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		s.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	// Try and marshall the job Json. This shouldn't go wrong but if it does, it's not a fatal error
	// Rather it means that the json won't be available in the ui
	var jobProto []byte
	apiJob, err := eventutil.ApiJobFromLogSubmitJob(owner, []string{}, queue, jobSet, ts, event)
	if err == nil {

		jobProtoUncompressed, err := proto.Marshal(apiJob)
		if err != nil {
			logger.Warnf("Couldn't marshall job %s in jobset %s as json.  %+v", jobId, jobSet, err)
		}

		jobProto, err = compressor.Compress(jobProtoUncompressed)
		if err != nil {
			logger.Warnf("Couldn't compress proto for job %s in jobset %s as json.  %+v", jobId, jobSet, err)
		}
	} else {
		s.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		logger.Warnf("Couldn't convert job event for job %s in jobset %s to api job.  %+v", jobId, jobSet, err)
	}

	resources := getJobResources(apiJob)
	priorityClass := getJobPriorityClass(apiJob)

	job := model.CreateJobInstruction{
		JobId:                     jobId,
		Queue:                     queue,
		Owner:                     owner,
		JobSet:                    jobSet,
		Cpu:                       resources.Cpu,
		Memory:                    resources.Memory,
		EphemeralStorage:          resources.EphemeralStorage,
		Gpu:                       resources.Gpu,
		Priority:                  int64(event.Priority),
		Submitted:                 ts,
		LastTransitionTime:        ts,
		LastTransitionTimeSeconds: ts.Unix(),
		State:                     database.JobQueuedOrdinal,
		JobProto:                  jobProto,
		PriorityClass:             priorityClass,
	}
	update.JobsToCreate = append(update.JobsToCreate, &job)

	annotationInstructions := extractAnnotations(jobId, queue, jobSet, event.GetObjectMeta().GetAnnotations(), userAnnotationPrefix)
	update.UserAnnotationsToCreate = append(update.UserAnnotationsToCreate, annotationInstructions...)

	return err
}

func extractAnnotations(jobId string, queue string, jobset string, jobAnnotations map[string]string, userAnnotationPrefix string) []*model.CreateUserAnnotationInstruction {
	// This intermediate variable exists because we want our output to be deterministic
	// Iteration over a map in go is non-deterministic, so we read everything into annotations
	// and then sort it.
	annotations := make([]*model.CreateUserAnnotationInstruction, 0, len(jobAnnotations))

	for k, v := range jobAnnotations {
		if k != "" {
			// The annotation will have a key with a prefix.  We want to strip the prefix before storing in the db
			if strings.HasPrefix(k, userAnnotationPrefix) && len(k) > len(userAnnotationPrefix) {
				k = k[len(userAnnotationPrefix):]
			}
			annotations = append(annotations, &model.CreateUserAnnotationInstruction{
				JobId:  jobId,
				Key:    k,
				Value:  v,
				Queue:  queue,
				Jobset: jobset,
			})
		} else {
			log.WithField("JobId", jobId).Warnf("Ignoring annotation with empty key")
		}
	}

	// sort to make output deterministic
	sort.Slice(annotations, func(i, j int) bool {
		return annotations[i].Key < annotations[j].Key
	})
	return annotations
}

func (s *Service) handleReprioritiseJob(ts time.Time, event *armadaevents.ReprioritisedJob, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		s.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	jobUpdate := model.UpdateJobInstruction{
		JobId:    jobId,
		Priority: pointer.Int64(int64(event.Priority)),
	}
	update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	return nil
}

func (s *Service) handleJobDuplicateDetected(ts time.Time, event *armadaevents.JobDuplicateDetected, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetNewJobId())
	if err != nil {
		s.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	jobUpdate := model.UpdateJobInstruction{
		JobId:     jobId,
		Duplicate: pointer.Bool(true),
	}
	update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	return nil
}

func (s *Service) handleCancelJob(ts time.Time, event *armadaevents.CancelledJob, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		s.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	jobUpdate := model.UpdateJobInstruction{
		JobId:                     jobId,
		State:                     pointer.Int32(int32(database.JobCancelledOrdinal)),
		Cancelled:                 &ts,
		LastTransitionTime:        &ts,
		LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
	}
	update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	return nil
}

func (s *Service) handleJobSucceeded(ts time.Time, event *armadaevents.JobSucceeded, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		s.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	jobUpdate := model.UpdateJobInstruction{
		JobId:                     jobId,
		State:                     pointer.Int32(int32(database.JobSucceededOrdinal)),
		LastTransitionTime:        &ts,
		LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
	}
	update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	return nil
}

func (s *Service) handleJobErrors(ts time.Time, event *armadaevents.JobErrors, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		s.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	isTerminal := false

	for _, e := range event.GetErrors() {
		if e.Terminal {
			isTerminal = true
			break
		}
	}

	if isTerminal {
		jobUpdate := model.UpdateJobInstruction{
			JobId:                     jobId,
			State:                     pointer.Int32(int32(database.JobFailedOrdinal)),
			LastTransitionTime:        &ts,
			LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
		}
		update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	}
	return nil
}

func (s *Service) handleJobRunRunning(ts time.Time, event *armadaevents.JobRunRunning, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		s.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	runId, err := armadaevents.UuidStringFromProtoUuid(event.GetRunId())
	if err != nil {
		s.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	// Update Job
	job := model.UpdateJobInstruction{
		JobId:                     jobId,
		State:                     pointer.Int32(int32(database.JobRunningOrdinal)),
		LastTransitionTime:        &ts,
		LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
		LatestRunId:               &runId,
	}

	update.JobsToUpdate = append(update.JobsToUpdate, &job)

	// Update Job Run
	node := getNode(event.ResourceInfos)
	jobRun := model.UpdateJobRunInstruction{
		RunId:       runId,
		Node:        &node,
		Started:     &ts,
		JobRunState: pointer.Int32(database.JobRunRunningOrdinal),
	}
	update.JobRunsToUpdate = append(update.JobRunsToUpdate, &jobRun)
	return nil
}

func (s *Service) handleJobRunAssigned(ts time.Time, event *armadaevents.JobRunAssigned, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		s.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	runId, err := armadaevents.UuidStringFromProtoUuid(event.RunId)
	if err != nil {
		s.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	// Update Job
	job := model.UpdateJobInstruction{
		JobId:                     jobId,
		State:                     pointer.Int32(int32(database.JobPendingOrdinal)),
		LastTransitionTime:        &ts,
		LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
		LatestRunId:               &runId,
	}

	update.JobsToUpdate = append(update.JobsToUpdate, &job)
	cluster := ""
	if len(event.GetResourceInfos()) > 0 {
		cluster = event.GetResourceInfos()[0].GetObjectMeta().GetExecutorId()
	}
	// Now create a job run
	jobRun := model.CreateJobRunInstruction{
		RunId:       runId,
		JobId:       jobId,
		Cluster:     cluster,
		Pending:     ts,
		JobRunState: database.JobRunPendingOrdinal,
	}
	update.JobRunsToCreate = append(update.JobRunsToCreate, &jobRun)
	return nil
}

func (s *Service) handleJobRunSucceeded(ts time.Time, event *armadaevents.JobRunSucceeded, update *model.InstructionSet) error {
	runId, err := armadaevents.UuidStringFromProtoUuid(event.RunId)
	if err != nil {
		s.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return errors.WithStack(err)
	}

	jobRun := model.UpdateJobRunInstruction{
		RunId:       runId,
		Finished:    &ts,
		JobRunState: pointer.Int32(database.JobRunSucceededOrdinal),
		ExitCode:    pointer.Int32(0),
	}
	update.JobRunsToUpdate = append(update.JobRunsToUpdate, &jobRun)
	return nil
}

func (s *Service) handleJobRunErrors(ts time.Time, logger logrus.FieldLogger, event *armadaevents.JobRunErrors, update *model.InstructionSet, compressor compress.Compressor) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		s.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return errors.WithStack(err)
	}

	runId, err := armadaevents.UuidStringFromProtoUuid(event.RunId)
	if err != nil {
		s.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return errors.WithStack(err)
	}

	for _, e := range event.GetErrors() {
		// We just interpret the first terminal error
		if e.Terminal {

			// Certain legacy events mean we don't have a valid run id
			// In this case we have to invent a fake run
			// TODO: remove this when the legacy messages go away!
			isLegacyEvent := runId == eventutil.LEGACY_RUN_ID
			if isLegacyEvent {
				jobRun := createFakeJobRun(jobId, ts)
				runId = jobRun.RunId
				objectMeta := extractMetaFromError(e)
				if objectMeta != nil && objectMeta.ExecutorId != "" {
					jobRun.Cluster = objectMeta.ExecutorId
				}
				update.JobRunsToCreate = append(update.JobRunsToCreate, jobRun)
			}

			jobRunUpdate := &model.UpdateJobRunInstruction{
				RunId:    runId,
				Finished: &ts,
			}
			if isLegacyEvent {
				jobRunUpdate.Started = &ts
			}

			switch reason := e.Reason.(type) {
			case *armadaevents.Error_PodError:
				jobRunUpdate.Node = extractNodeName(reason.PodError)
				jobRunUpdate.JobRunState = pointer.Int32(database.JobRunFailedOrdinal)
				jobRunUpdate.Error = tryCompressError(jobId, reason.PodError.GetMessage(), compressor, logger)
				var exitCode int32 = 0
				for _, containerError := range reason.PodError.ContainerErrors {
					if containerError.ExitCode != 0 {
						exitCode = containerError.ExitCode
						break
					}
				}
				jobRunUpdate.ExitCode = pointer.Int32(exitCode)
			case *armadaevents.Error_PodTerminated:
				jobRunUpdate.Node = extractNodeName(reason.PodTerminated)
				jobRunUpdate.JobRunState = pointer.Int32(database.JobRunTerminatedOrdinal)
				jobRunUpdate.Error = tryCompressError(jobId, reason.PodTerminated.GetMessage(), compressor, logger)
			case *armadaevents.Error_PodUnschedulable:
				jobRunUpdate.Node = extractNodeName(reason.PodUnschedulable)
				jobRunUpdate.JobRunState = pointer.Int32(database.JobRunUnableToScheduleOrdinal)
				jobRunUpdate.Error = tryCompressError(jobId, reason.PodUnschedulable.GetMessage(), compressor, logger)
			case *armadaevents.Error_PodLeaseReturned:
				jobRunUpdate.JobRunState = pointer.Int32(database.JobRunLeaseReturnedOrdinal)
				jobRunUpdate.Error = tryCompressError(jobId, reason.PodLeaseReturned.GetMessage(), compressor, logger)
			case *armadaevents.Error_LeaseExpired:
				jobRunUpdate.JobRunState = pointer.Int32(database.JobRunLeaseExpiredOrdinal)
				jobRunUpdate.Error = tryCompressError(jobId, "Lease expired", compressor, logger)
			default:
				jobRunUpdate.JobRunState = pointer.Int32(database.JobRunFailedOrdinal)
				jobRunUpdate.Error = tryCompressError(jobId, "Unknown error", compressor, logger)
				log.Debugf("Ignoring event %T", reason)
			}
			update.JobRunsToUpdate = append(update.JobRunsToUpdate, jobRunUpdate)
			break
		}
	}
	return nil
}

func tryCompressError(jobId string, errorString string, compressor compress.Compressor, logger logrus.FieldLogger) []byte {
	compressedError, err := compressor.Compress([]byte(util.RemoveNullsFromString(errorString)))
	if err != nil {
		logger.Warnf("Couldn't compress error for job %s as json.  %+v", jobId, err)
	}
	return compressedError
}

func extractMetaFromError(e *armadaevents.Error) *armadaevents.ObjectMeta {
	switch err := e.Reason.(type) {
	case *armadaevents.Error_PodError:
		return err.PodError.ObjectMeta
	case *armadaevents.Error_PodTerminated:
		return err.PodTerminated.ObjectMeta
	case *armadaevents.Error_PodUnschedulable:
		return err.PodUnschedulable.ObjectMeta
	case *armadaevents.Error_PodLeaseReturned:
		return err.PodLeaseReturned.ObjectMeta
	}
	return nil
}

func getNode(resources []*armadaevents.KubernetesResourceInfo) string {
	for _, r := range resources {
		node := r.GetPodInfo().GetNodeName()
		if node != "" {
			return node
		}
	}
	return "UNKNOWN"
}

func createFakeJobRun(jobId string, ts time.Time) *model.CreateJobRunInstruction {
	runId := uuid.New().String()
	return &model.CreateJobRunInstruction{
		RunId:       runId,
		JobId:       jobId,
		Cluster:     "UNKNOWN",
		Pending:     ts,
		JobRunState: database.JobRunPendingOrdinal,
	}
}

func extractNodeName(x HasNodeName) *string {
	nodeName := x.GetNodeName()
	if len(nodeName) > 0 {
		return pointer.String(nodeName)
	}
	return nil
}

func getJobResources(job *api.Job) jobResources {
	resources := jobResources{}

	podSpec := util.PodSpecFromJob(job)

	for _, container := range podSpec.Containers {
		resources.Cpu += getResource(container, v1.ResourceCPU, true)
		resources.Memory += getResource(container, v1.ResourceMemory, false)
		resources.EphemeralStorage += getResource(container, v1.ResourceEphemeralStorage, false)
		resources.Gpu += getResource(container, "nvidia.com/gpu", false)
	}

	return resources
}

func getResource(container v1.Container, resourceName v1.ResourceName, useMillis bool) int64 {
	resource, ok := container.Resources.Requests[resourceName]
	if !ok {
		return 0
	}
	if useMillis {
		return resource.MilliValue()
	}
	return resource.Value()
}

func getJobPriorityClass(job *api.Job) *string {
	podSpec := util.PodSpecFromJob(job)
	if podSpec.PriorityClassName != "" {
		return pointer.String(podSpec.PriorityClassName)
	}
	return nil
}

// Put the requestId into a message-specific context and logger, which are passed on to sub-functions.
func createCtxWithLogger(ctx context.Context, pulsarMsg pulsar.Message) (context.Context, logrus.FieldLogger) {
	requestId := pulsarrequestid.FromMessageOrMissing(pulsarMsg)
	messageCtx, ok := requestid.AddToIncomingContext(ctx, requestId)
	if !ok {
		messageCtx = ctx
	}
	messageLogger := log.WithFields(logrus.Fields{"messageId": pulsarMsg.ID(), requestid.MetadataKey: requestId})
	ctxWithLogger := ctxlogrus.ToContext(messageCtx, messageLogger)

	return ctxWithLogger, messageLogger
}
