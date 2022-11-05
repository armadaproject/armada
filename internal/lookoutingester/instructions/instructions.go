package instructions

import (
	"sort"
	"strings"
	"time"

	"github.com/G-Research/armada/internal/common/ingest/metrics"

	"github.com/G-Research/armada/internal/common/ingest"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"k8s.io/utils/pointer"

	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/lookout/repository"
	"github.com/G-Research/armada/internal/lookoutingester/model"
	"github.com/G-Research/armada/pkg/armadaevents"
)

type HasNodeName interface {
	GetNodeName() string
}

type InstructionCenverter struct {
	metrics              *metrics.Metrics
	userAnnotationPrefix string
	compressor           compress.Compressor
}

func NewInstructionConverter(metrics *metrics.Metrics, userAnnotationPrefix string, compressor compress.Compressor) ingest.InstructionConverter[*model.InstructionSet] {
	return &InstructionCenverter{
		metrics:              metrics,
		userAnnotationPrefix: userAnnotationPrefix,
		compressor:           compressor,
	}
}

func (c *InstructionCenverter) Convert(sequencesWithIds *ingest.EventSequencesWithIds) *model.InstructionSet {
	updateInstructions := &model.InstructionSet{
		MessageIds: sequencesWithIds.MessageIds,
	}

	for _, es := range sequencesWithIds.EventSequences {
		c.convertSequence(es, updateInstructions)
	}
	return updateInstructions
}

func (c *InstructionCenverter) convertSequence(es *armadaevents.EventSequence, update *model.InstructionSet) {
	queue := es.Queue
	jobset := es.JobSetName
	owner := es.UserId
	for idx, event := range es.Events {
		var err error = nil
		switch event.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_SubmitJob:
			err = c.handleSubmitJob(queue, owner, jobset, *event.Created, event.GetSubmitJob(), update)
		case *armadaevents.EventSequence_Event_ReprioritisedJob:
			err = c.handleReprioritiseJob(*event.Created, event.GetReprioritisedJob(), update)
		case *armadaevents.EventSequence_Event_CancelledJob:
			err = c.handleCancelJob(*event.Created, event.GetCancelledJob(), update)
		case *armadaevents.EventSequence_Event_JobSucceeded:
			err = c.handleJobSucceeded(*event.Created, event.GetJobSucceeded(), update)
		case *armadaevents.EventSequence_Event_JobErrors:
			err = c.handleJobErrors(*event.Created, event.GetJobErrors(), update)
		case *armadaevents.EventSequence_Event_JobRunAssigned:
			err = c.handleJobRunAssigned(*event.Created, event.GetJobRunAssigned(), update)
		case *armadaevents.EventSequence_Event_JobRunRunning:
			err = c.handleJobRunRunning(*event.Created, event.GetJobRunRunning(), update)
		case *armadaevents.EventSequence_Event_JobRunSucceeded:
			err = c.handleJobRunSucceeded(*event.Created, event.GetJobRunSucceeded(), update)
		case *armadaevents.EventSequence_Event_JobRunErrors:
			err = c.handleJobRunErrors(*event.Created, event.GetJobRunErrors(), update)
		case *armadaevents.EventSequence_Event_JobDuplicateDetected:
			err = c.handleJobDuplicateDetected(*event.Created, event.GetJobDuplicateDetected(), update)
		case *armadaevents.EventSequence_Event_CancelJob,
			*armadaevents.EventSequence_Event_JobRunLeased,
			*armadaevents.EventSequence_Event_ReprioritiseJobSet,
			*armadaevents.EventSequence_Event_CancelJobSet,
			*armadaevents.EventSequence_Event_ResourceUtilisation,
			*armadaevents.EventSequence_Event_StandaloneIngressInfo,
			*armadaevents.EventSequence_Event_JobRunPreempted:
			log.Debugf("Ignoring event type %T", event)
		default:
			log.Warnf("Ignoring unknown event type %T", event)
		}
		if err != nil {
			c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
			log.WithError(err).Warnf("Could not convert event at index %d.", idx)
		}
	}
}

func (c *InstructionCenverter) handleSubmitJob(
	queue string,
	owner string,
	jobSet string,
	ts time.Time,
	event *armadaevents.SubmitJob,
	update *model.InstructionSet,
) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	// Try and marshall the job Json. This shouldn't go wrong but if it does, it'c not a fatal error
	// Rather it means that the json won't be available in the ui
	var jobProto []byte
	apiJob, err := eventutil.ApiJobFromLogSubmitJob(owner, []string{}, queue, jobSet, ts, event)
	if err == nil {

		jobProtoUncompressed, err := proto.Marshal(apiJob)
		if err != nil {
			log.WithError(err).Warnf("couldn't marshall job %s in jobset %s as json", jobId, jobSet)
		}

		jobProto, err = c.compressor.Compress(jobProtoUncompressed)
		if err != nil {
			log.WithError(err).Warnf("Couldn't compress proto for job %s in jobset %s as json.", jobId, jobSet)
		}
	} else {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		log.WithError(err).Warnf("Couldn't convert job event for job %s in jobset %s to api job.", jobId, jobSet)
	}

	job := model.CreateJobInstruction{
		JobId:     jobId,
		Queue:     queue,
		Owner:     owner,
		JobSet:    jobSet,
		Priority:  event.Priority,
		Submitted: ts,
		JobProto:  jobProto,
		State:     repository.JobQueuedOrdinal,
		Updated:   ts,
	}
	update.JobsToCreate = append(update.JobsToCreate, &job)

	annotationInstructions := extractAnnotations(jobId, event.GetObjectMeta().GetAnnotations(), c.userAnnotationPrefix)
	update.UserAnnotationsToCreate = append(update.UserAnnotationsToCreate, annotationInstructions...)

	return err
}

func extractAnnotations(jobId string, jobAnnotations map[string]string, userAnnotationPrefix string) []*model.CreateUserAnnotationInstruction {
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

func (c *InstructionCenverter) handleReprioritiseJob(ts time.Time, event *armadaevents.ReprioritisedJob, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
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

func (c *InstructionCenverter) handleJobDuplicateDetected(ts time.Time, event *armadaevents.JobDuplicateDetected, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetNewJobId())
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
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

func (c *InstructionCenverter) handleCancelJob(ts time.Time, event *armadaevents.CancelledJob, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
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

func (c *InstructionCenverter) handleJobSucceeded(ts time.Time, event *armadaevents.JobSucceeded, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
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

func (c *InstructionCenverter) handleJobErrors(ts time.Time, event *armadaevents.JobErrors, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
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
			JobId:   jobId,
			State:   pointer.Int32(int32(repository.JobFailedOrdinal)),
			Updated: ts,
		}
		update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	}
	return nil
}

func (c *InstructionCenverter) handleJobRunRunning(ts time.Time, event *armadaevents.JobRunRunning, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	runId, err := armadaevents.UuidStringFromProtoUuid(event.GetRunId())
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
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

func (c *InstructionCenverter) handleJobRunAssigned(ts time.Time, event *armadaevents.JobRunAssigned, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	runId, err := armadaevents.UuidStringFromProtoUuid(event.RunId)
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
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

func (c *InstructionCenverter) handleJobRunSucceeded(ts time.Time, event *armadaevents.JobRunSucceeded, update *model.InstructionSet) error {
	runId, err := armadaevents.UuidStringFromProtoUuid(event.RunId)
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
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

func (c *InstructionCenverter) handleJobRunErrors(ts time.Time, event *armadaevents.JobRunErrors, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return errors.WithStack(err)
	}

	runId, err := armadaevents.UuidStringFromProtoUuid(event.RunId)
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
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

			// Both Error_PodLeaseReturned and Error_LeaseExpired have an implied reset of the job state to queued
			// Ideally we would send an explicit queued message here, but until this change is made we correct the job
			// state here
			resetStateToQueued := false

			switch reason := e.Reason.(type) {
			case *armadaevents.Error_PodError:
				truncatedMsg := util.Truncate(util.RemoveNullsFromString(reason.PodError.GetMessage()), util.MaxMessageLength)
				jobRunUpdate.Error = pointer.String(truncatedMsg)
				jobRunUpdate.Node = extractNodeName(reason.PodError)
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
				jobRunUpdate.Node = extractNodeName(reason.PodTerminated)
			case *armadaevents.Error_PodUnschedulable:
				truncatedMsg := util.Truncate(util.RemoveNullsFromString(reason.PodUnschedulable.GetMessage()), util.MaxMessageLength)
				jobRunUpdate.Error = pointer.String(truncatedMsg)
				jobRunUpdate.UnableToSchedule = pointer.Bool(true)
				jobRunUpdate.Node = extractNodeName(reason.PodUnschedulable)
			case *armadaevents.Error_PodLeaseReturned:
				truncatedMsg := util.Truncate(util.RemoveNullsFromString(reason.PodLeaseReturned.GetMessage()), util.MaxMessageLength)
				jobRunUpdate.Error = pointer.String(truncatedMsg)
				jobRunUpdate.UnableToSchedule = pointer.Bool(true)
				resetStateToQueued = true
			case *armadaevents.Error_LeaseExpired:
				jobRunUpdate.Error = pointer.String("Lease Expired")
				jobRunUpdate.UnableToSchedule = pointer.Bool(true)
				resetStateToQueued = true
			default:
				jobRunUpdate.Error = pointer.String("Unknown error")
				log.Debugf("Ignoring event %T", reason)
			}
			update.JobRunsToUpdate = append(update.JobRunsToUpdate, jobRunUpdate)
			if resetStateToQueued {
				update.JobsToUpdate = append(update.JobsToUpdate, &model.UpdateJobInstruction{
					JobId:   jobId,
					State:   pointer.Int32(int32(repository.JobQueuedOrdinal)),
					Updated: ts,
				})
			}
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

func extractNodeName(x HasNodeName) *string {
	nodeName := x.GetNodeName()
	if len(nodeName) > 0 {
		return pointer.String(nodeName)
	}
	return nil
}
