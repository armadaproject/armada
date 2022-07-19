package instructions

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"k8s.io/utils/pointer"

	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/common/requestid"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/lookout/repository"
	"github.com/G-Research/armada/internal/lookoutingester/model"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/internal/pulsarutils/pulsarrequestid"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// Convert takes a channel containing incoming pulsar messages and returns a channel with the corresponding
// InstructionSets.  Each pulsar message will generate exactly one InstructionSet.
func Convert(ctx context.Context, msgs chan *pulsarutils.ConsumerMessage, bufferSize int, userAnnotationPrefix string, compressor compress.Compressor) chan *model.InstructionSet {
	out := make(chan *model.InstructionSet, bufferSize)
	go func() {
		for msg := range msgs {
			instructions := ConvertMsg(ctx, msg, userAnnotationPrefix, compressor)
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
func ConvertMsg(ctx context.Context, msg *pulsarutils.ConsumerMessage, userAnnotationPrefix string, compressor compress.Compressor) *model.InstructionSet {

	pulsarMsg := msg.Message

	// Put the requestId into a message-specific context and logger,
	// which are passed on to sub-functions.
	requestId := pulsarrequestid.FromMessageOrMissing(pulsarMsg)
	messageCtx, ok := requestid.AddToIncomingContext(ctx, requestId)
	if !ok {
		messageCtx = ctx
	}
	messageLogger := log.WithFields(logrus.Fields{"messageId": pulsarMsg.ID(), requestid.MetadataKey: requestId})
	ctxWithLogger := ctxlogrus.ToContext(messageCtx, messageLogger)
	updateInstructions := &model.InstructionSet{
		MessageIds: []*pulsarutils.ConsumerMessageId{
			{pulsarMsg.ID(), 0, msg.ConsumerId},
		},
	}

	// It's not a control message-no instructions needed
	if !armadaevents.IsControlMessage(msg.Message) {
		return updateInstructions
	}

	// Try and unmarshall the proto-  if it fails there's not much we can do here.
	sequence, err := eventutil.UnmarshalEventSequence(ctxWithLogger, pulsarMsg.Payload())
	if err != nil {
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
			err = handleSubmitJob(messageLogger, queue, owner, jobset, ts, event.GetSubmitJob(), userAnnotationPrefix, compressor, updateInstructions)
		case *armadaevents.EventSequence_Event_ReprioritisedJob:
			err = handleReprioritiseJob(ts, event.GetReprioritisedJob(), updateInstructions)
		case *armadaevents.EventSequence_Event_CancelledJob:
			err = handleCancelJob(ts, event.GetCancelledJob(), updateInstructions)
		case *armadaevents.EventSequence_Event_JobSucceeded:
			err = handleJobSucceeded(ts, event.GetJobSucceeded(), updateInstructions)
		case *armadaevents.EventSequence_Event_JobErrors:
			err = handleJobErrors(ts, event.GetJobErrors(), updateInstructions)
		case *armadaevents.EventSequence_Event_JobRunAssigned:
			err = handleJobRunAssigned(ts, event.GetJobRunAssigned(), updateInstructions)
		case *armadaevents.EventSequence_Event_JobRunRunning:
			err = handleJobRunRunning(ts, event.GetJobRunRunning(), updateInstructions)
		case *armadaevents.EventSequence_Event_JobRunSucceeded:
			err = handleJobRunSucceeded(ts, event.GetJobRunSucceeded(), updateInstructions)
		case *armadaevents.EventSequence_Event_JobRunErrors:
			err = handleJobRunErrors(ts, event.GetJobRunErrors(), updateInstructions)
		case *armadaevents.EventSequence_Event_JobDuplicateDetected:
			err = handleJobDuplicateDetected(ts, event.GetJobDuplicateDetected(), updateInstructions)
		case *armadaevents.EventSequence_Event_CancelJob:
		case *armadaevents.EventSequence_Event_JobRunLeased:
		case *armadaevents.EventSequence_Event_ReprioritiseJobSet:
		case *armadaevents.EventSequence_Event_CancelJobSet:
			messageLogger.Debugf("Ignoring event")
		default:
			messageLogger.Warnf("Ignoring event")
		}
		if err != nil {
			messageLogger.Warnf("Could not convert event at index %d. %+v", idx, err)
		}
	}
	return updateInstructions
}

func handleSubmitJob(logger *logrus.Entry, queue string, owner string, jobSet string, ts time.Time, event *armadaevents.SubmitJob, userAnnotationPrefix string, compressor compress.Compressor, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		return err
	}

	// Try and marshall the job Json. This shouldn't go wrong but if it does, it's not a fatal error
	// Rather it means that the json won't be available in the ui
	var jobJson []byte
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

		// TODO: Remove this when we have moved over to compressed proto
		jobJson, err = json.Marshal(apiJob)
		if err != nil {
			logger.Warnf("Couldn't marshall job json %s in jobset %s as json.  %+v", jobId, jobSet, err)
		}

	} else {
		logger.Warnf("Couldn't convert job event for job %s in jobset %s to api job.  %+v", jobId, jobSet, err)
	}

	job := model.CreateJobInstruction{
		JobId:     jobId,
		Queue:     queue,
		Owner:     owner,
		JobSet:    jobSet,
		Priority:  event.Priority,
		Submitted: ts,
		JobJson:   util.RemoveNullsFromJson(jobJson),
		JobProto:  jobProto,
		State:     repository.JobQueuedOrdinal,
		Updated:   ts,
	}
	update.JobsToCreate = append(update.JobsToCreate, &job)

	annotationInstructions := extractAnnotations(jobId, event.GetObjectMeta().GetAnnotations(), userAnnotationPrefix)
	update.UserAnnotationsToCreate = append(update.UserAnnotationsToCreate, annotationInstructions...)

	return err
}

func extractAnnotations(jobId string, jobAnnotations map[string]string, userAnnotationPrefix string) []*model.CreateUserAnnotationInstruction {

	// This intermediate variable exists because we want our output to be deterministic
	// Iteration over a map in go is non-deterministic so we read everything into annotations
	// and then sort it.
	annotations := make([]*model.CreateUserAnnotationInstruction, 0, len(jobAnnotations))

	for k, v := range jobAnnotations {
		if k != "" {
			// The annotation will have a key with a prefix.  We want to strip the prefix before storing in the db
			if strings.HasPrefix(k, userAnnotationPrefix) && len(k) > len(userAnnotationPrefix) {
				k = k[len(userAnnotationPrefix):]
			}
			annotations = append(annotations, &model.CreateUserAnnotationInstruction{
				JobId: jobId,
				Key:   k,
				Value: v,
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

func handleReprioritiseJob(ts time.Time, event *armadaevents.ReprioritisedJob, update *model.InstructionSet) error {

	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		return err
	}

	jobUpdate := model.UpdateJobInstruction{
		JobId:    jobId,
		Priority: pointer.Int32(int32(event.Priority)),
		Updated:  ts,
	}
	update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	return nil
}

func handleJobDuplicateDetected(ts time.Time, event *armadaevents.JobDuplicateDetected, update *model.InstructionSet) error {

	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetNewJobId())
	if err != nil {
		return err
	}

	jobUpdate := model.UpdateJobInstruction{
		JobId:     jobId,
		Duplicate: pointer.Bool(true),
		Updated:   ts,
	}
	update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	return nil
}

func handleCancelJob(ts time.Time, event *armadaevents.CancelledJob, update *model.InstructionSet) error {

	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		return err
	}

	jobUpdate := model.UpdateJobInstruction{
		JobId:     jobId,
		State:     pointer.Int32(int32(repository.JobCancelledOrdinal)),
		Cancelled: &ts,
		Updated:   ts,
	}
	update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	return nil
}

func handleJobSucceeded(ts time.Time, event *armadaevents.JobSucceeded, update *model.InstructionSet) error {

	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		return err
	}

	jobUpdate := model.UpdateJobInstruction{
		JobId:   jobId,
		State:   pointer.Int32(int32(repository.JobSucceededOrdinal)),
		Updated: ts,
	}
	update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	return nil
}

func handleJobErrors(ts time.Time, event *armadaevents.JobErrors, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		return err
	}

	var isTerminal = false

	for _, e := range event.GetErrors() {
		if e.Terminal {
			isTerminal = true
			break
		}
	}
	if isTerminal {
		jobUpdate := model.UpdateJobInstruction{
			JobId:   jobId,
			State:   pointer.Int32(int32(repository.JobFailedOrdinal)),
			Updated: ts,
		}
		update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	}
	return nil
}

func handleJobRunRunning(ts time.Time, event *armadaevents.JobRunRunning, update *model.InstructionSet) error {

	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		return err
	}

	runId, err := armadaevents.UuidStringFromProtoUuid(event.GetRunId())

	if err != nil {
		return err
	}

	// Update Job
	job := model.UpdateJobInstruction{
		JobId:   jobId,
		State:   pointer.Int32(int32(repository.JobRunningOrdinal)),
		Updated: ts,
	}

	update.JobsToUpdate = append(update.JobsToUpdate, &job)

	// Update Job Run
	node, podNumber := getNode(event.ResourceInfos)
	jobRun := model.UpdateJobRunInstruction{
		RunId:     runId,
		Started:   &ts,
		Node:      &node,
		PodNumber: pointer.Int32(int32(podNumber)),
	}
	update.JobRunsToUpdate = append(update.JobRunsToUpdate, &jobRun)
	return nil
}

func handleJobRunAssigned(ts time.Time, event *armadaevents.JobRunAssigned, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		return err
	}

	runId, err := armadaevents.UuidStringFromProtoUuid(event.RunId)
	if err != nil {
		return err
	}

	// Update Job
	job := model.UpdateJobInstruction{
		JobId:   jobId,
		State:   pointer.Int32(int32(repository.JobPendingOrdinal)),
		Updated: ts,
	}

	update.JobsToUpdate = append(update.JobsToUpdate, &job)
	cluster := ""
	if len(event.GetResourceInfos()) > 0 {
		cluster = event.GetResourceInfos()[0].GetObjectMeta().GetExecutorId()
	}
	// Now create a job run
	jobRun := model.CreateJobRunInstruction{
		RunId:   runId,
		JobId:   jobId,
		Cluster: cluster,
		Created: ts,
	}
	update.JobRunsToCreate = append(update.JobRunsToCreate, &jobRun)
	return nil
}

func handleJobRunSucceeded(ts time.Time, event *armadaevents.JobRunSucceeded, update *model.InstructionSet) error {

	runId, err := armadaevents.UuidStringFromProtoUuid(event.RunId)
	if err != nil {
		return errors.WithStack(err)
	}

	jobRun := model.UpdateJobRunInstruction{
		RunId:     runId,
		Succeeded: pointer.Bool(true),
		Finished:  &ts,
	}
	update.JobRunsToUpdate = append(update.JobRunsToUpdate, &jobRun)
	return nil
}

func handleJobRunErrors(ts time.Time, event *armadaevents.JobRunErrors, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		return errors.WithStack(err)
	}

	runId, err := armadaevents.UuidStringFromProtoUuid(event.RunId)
	if err != nil {
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
				RunId:     runId,
				Succeeded: pointer.Bool(false),
				Finished:  &ts,
			}
			if isLegacyEvent {
				jobRunUpdate.Started = &ts
			}

			switch reason := e.Reason.(type) {
			case *armadaevents.Error_PodError:
				truncatedMsg := util.Truncate(util.RemoveNullsFromString(reason.PodError.GetMessage()), util.MaxMessageLength)
				jobRunUpdate.Error = pointer.String(truncatedMsg)
				jobRunUpdate.Node = pointer.String(reason.PodError.NodeName)
				for _, containerError := range reason.PodError.ContainerErrors {
					update.JobRunContainersToCreate = append(update.JobRunContainersToCreate, &model.CreateJobRunContainerInstruction{
						RunId:         jobRunUpdate.RunId,
						ExitCode:      containerError.ExitCode,
						ContainerName: containerError.GetObjectMeta().GetName(),
					})
				}
			case *armadaevents.Error_PodTerminated:
				truncatedMsg := util.Truncate(util.RemoveNullsFromString(reason.PodTerminated.GetMessage()), util.MaxMessageLength)
				jobRunUpdate.Error = pointer.String(truncatedMsg)
				jobRunUpdate.Node = pointer.String(reason.PodTerminated.NodeName)
			case *armadaevents.Error_PodUnschedulable:
				truncatedMsg := util.Truncate(util.RemoveNullsFromString(reason.PodUnschedulable.GetMessage()), util.MaxMessageLength)
				jobRunUpdate.Error = pointer.String(truncatedMsg)
				jobRunUpdate.UnableToSchedule = pointer.Bool(true)
				jobRunUpdate.Node = pointer.String(reason.PodUnschedulable.NodeName)
			case *armadaevents.Error_PodLeaseReturned:
				truncatedMsg := util.Truncate(util.RemoveNullsFromString(reason.PodLeaseReturned.GetMessage()), util.MaxMessageLength)
				jobRunUpdate.Error = pointer.String(truncatedMsg)
				jobRunUpdate.UnableToSchedule = pointer.Bool(true)
			case *armadaevents.Error_LeaseExpired:
				jobRunUpdate.Error = pointer.String("Lease Expired")
				jobRunUpdate.UnableToSchedule = pointer.Bool(true)
			default:
				jobRunUpdate.Error = pointer.String("Unknown error")
				log.Debugf("Ignoring event %T", reason)
			}
			update.JobRunsToUpdate = append(update.JobRunsToUpdate, jobRunUpdate)
			break
		}
	}
	return nil
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

func getNode(resources []*armadaevents.KubernetesResourceInfo) (string, int) {
	for _, r := range resources {
		node := r.GetPodInfo().GetNodeName()
		if node != "" {
			return node, int(r.GetPodInfo().GetPodNumber())
		}
	}
	return "UNKNOWN", -1
}

func createFakeJobRun(jobId string, ts time.Time) *model.CreateJobRunInstruction {
	runId := uuid.New().String()
	return &model.CreateJobRunInstruction{
		RunId:   runId,
		JobId:   jobId,
		Cluster: "UNKNOWN",
		Created: ts,
	}
}
