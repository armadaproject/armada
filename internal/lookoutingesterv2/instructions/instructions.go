package instructions

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/model"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	maxQueueLen         = 512
	maxOwnerLen         = 512
	maxJobSetLen        = 1024
	maxPriorityClassLen = 63
	maxClusterLen       = 512
	maxNodeLen          = 512
)

type HasNodeName interface {
	GetNodeName() string
}

type InstructionConverter struct {
	metrics              *metrics.Metrics
	userAnnotationPrefix string
	compressor           compress.Compressor
}

type jobResources struct {
	Cpu              int64
	Memory           int64
	EphemeralStorage int64
	Gpu              int64
}

func NewInstructionConverter(m *metrics.Metrics, userAnnotationPrefix string, compressor compress.Compressor) *InstructionConverter {
	return &InstructionConverter{metrics: m, userAnnotationPrefix: userAnnotationPrefix, compressor: compressor}
}

func (c *InstructionConverter) Convert(ctx context.Context, sequencesWithIds *ingest.EventSequencesWithIds) *model.InstructionSet {
	updateInstructions := &model.InstructionSet{
		MessageIds: sequencesWithIds.MessageIds,
	}

	for _, es := range sequencesWithIds.EventSequences {
		c.convertSequence(ctx, es, updateInstructions)
	}
	return updateInstructions
}

func (c *InstructionConverter) convertSequence(
	ctx context.Context,
	sequence *armadaevents.EventSequence,
	update *model.InstructionSet,
) {
	queue := util.Truncate(sequence.Queue, maxQueueLen)
	jobset := util.Truncate(sequence.JobSetName, maxJobSetLen)
	owner := util.Truncate(sequence.UserId, maxOwnerLen)
	for idx, event := range sequence.Events {
		var err error
		if event.Created == nil {
			c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
			log.WithError(err).Warnf("Missing timestamp for event at index %d.", idx)
			continue
		}
		ts := *event.Created
		switch event.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_SubmitJob:
			err = c.handleSubmitJob(queue, owner, jobset, ts, event.GetSubmitJob(), update)
		case *armadaevents.EventSequence_Event_ReprioritisedJob:
			err = c.handleReprioritiseJob(ts, event.GetReprioritisedJob(), update)
		case *armadaevents.EventSequence_Event_CancelledJob:
			err = c.handleCancelledJob(ts, event.GetCancelledJob(), update)
		case *armadaevents.EventSequence_Event_JobSucceeded:
			err = c.handleJobSucceeded(ts, event.GetJobSucceeded(), update)
		case *armadaevents.EventSequence_Event_JobErrors:
			err = c.handleJobErrors(ts, event.GetJobErrors(), update)
		case *armadaevents.EventSequence_Event_JobRunAssigned:
			err = c.handleJobRunAssigned(ts, event.GetJobRunAssigned(), update)
		case *armadaevents.EventSequence_Event_JobRunRunning:
			err = c.handleJobRunRunning(ts, event.GetJobRunRunning(), update)
		case *armadaevents.EventSequence_Event_JobRunSucceeded:
			err = c.handleJobRunSucceeded(ts, event.GetJobRunSucceeded(), update)
		case *armadaevents.EventSequence_Event_JobRunErrors:
			err = c.handleJobRunErrors(ts, event.GetJobRunErrors(), update)
		case *armadaevents.EventSequence_Event_JobDuplicateDetected:
			err = c.handleJobDuplicateDetected(ts, event.GetJobDuplicateDetected(), update)
		case *armadaevents.EventSequence_Event_JobRunPreempted:
			err = c.handleJobRunPreempted(ts, event.GetJobRunPreempted(), update)
		case *armadaevents.EventSequence_Event_JobRunLeased:
		case *armadaevents.EventSequence_Event_ReprioritiseJobSet:
		case *armadaevents.EventSequence_Event_CancelJobSet:
		case *armadaevents.EventSequence_Event_ResourceUtilisation:
		case *armadaevents.EventSequence_Event_StandaloneIngressInfo:
		case *armadaevents.EventSequence_Event_PartitionMarker:
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

func (c *InstructionConverter) handleSubmitJob(
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
	if event.IsDuplicate {
		log.Debugf("job %s is a duplicate, ignoring", jobId)
		return nil
	}

	// Try and marshall the job proto. This shouldn't go wrong but if it does, it's not a fatal error
	// Rather it means that the job spec won't be available in the ui
	var jobProto []byte
	apiJob, err := eventutil.ApiJobFromLogSubmitJob(owner, []string{}, queue, jobSet, ts, event)
	if err == nil {

		jobProtoUncompressed, err := proto.Marshal(apiJob)
		if err != nil {
			log.WithError(err).Warnf("Couldn't marshall job %s in jobset %s as proto.", jobId, jobSet)
		}

		jobProto, err = c.compressor.Compress(jobProtoUncompressed)
		if err != nil {
			log.WithError(err).Warnf("Couldn't compress proto for job %s in jobset %s.", jobId, jobSet)
		}
	} else {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		log.WithError(err).Warnf("Couldn't convert job event for job %s in jobset %s to api job.", jobId, jobSet)
	}

	resources := getJobResources(apiJob)
	priorityClass := getJobPriorityClass(apiJob)
	if priorityClass != nil {
		truncatedPriorityClass := util.Truncate(*priorityClass, maxPriorityClassLen)
		priorityClass = &truncatedPriorityClass
	}

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
		State:                     lookout.JobQueuedOrdinal,
		JobProto:                  jobProto,
		PriorityClass:             priorityClass,
	}
	update.JobsToCreate = append(update.JobsToCreate, &job)

	annotationInstructions := extractAnnotations(jobId, queue, jobSet, event.GetObjectMeta().GetAnnotations(), c.userAnnotationPrefix)
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

func (c *InstructionConverter) handleReprioritiseJob(ts time.Time, event *armadaevents.ReprioritisedJob, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	jobUpdate := model.UpdateJobInstruction{
		JobId:    jobId,
		Priority: pointer.Int64(int64(event.Priority)),
	}
	update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	return nil
}

func (c *InstructionConverter) handleJobDuplicateDetected(ts time.Time, event *armadaevents.JobDuplicateDetected, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetNewJobId())
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	jobUpdate := model.UpdateJobInstruction{
		JobId:     jobId,
		Duplicate: pointer.Bool(true),
	}
	update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	return nil
}

func (c *InstructionConverter) handleCancelledJob(ts time.Time, event *armadaevents.CancelledJob, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	var reason *string
	if event.Reason != "" {
		reason = &event.Reason
	}
	jobUpdate := model.UpdateJobInstruction{
		JobId:                     jobId,
		State:                     pointer.Int32(int32(lookout.JobCancelledOrdinal)),
		Cancelled:                 &ts,
		CancelReason:              reason,
		LastTransitionTime:        &ts,
		LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
	}
	update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	return nil
}

func (c *InstructionConverter) handleJobSucceeded(ts time.Time, event *armadaevents.JobSucceeded, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.GetJobId())
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	jobUpdate := model.UpdateJobInstruction{
		JobId:                     jobId,
		State:                     pointer.Int32(int32(lookout.JobSucceededOrdinal)),
		LastTransitionTime:        &ts,
		LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
	}
	update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	return nil
}

func (c *InstructionConverter) handleJobErrors(ts time.Time, event *armadaevents.JobErrors, update *model.InstructionSet) error {
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
			JobId:                     jobId,
			State:                     pointer.Int32(int32(lookout.JobFailedOrdinal)),
			LastTransitionTime:        &ts,
			LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
		}
		update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	}
	return nil
}

func (c *InstructionConverter) handleJobRunRunning(ts time.Time, event *armadaevents.JobRunRunning, update *model.InstructionSet) error {
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
		JobId:                     jobId,
		State:                     pointer.Int32(int32(lookout.JobRunningOrdinal)),
		LastTransitionTime:        &ts,
		LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
		LatestRunId:               &runId,
	}

	update.JobsToUpdate = append(update.JobsToUpdate, &job)

	// Update Job Run
	node := getNode(event.ResourceInfos)
	jobRun := model.UpdateJobRunInstruction{
		RunId:       runId,
		Node:        node,
		Started:     &ts,
		JobRunState: pointer.Int32(lookout.JobRunRunningOrdinal),
	}
	update.JobRunsToUpdate = append(update.JobRunsToUpdate, &jobRun)
	return nil
}

func (c *InstructionConverter) handleJobRunAssigned(ts time.Time, event *armadaevents.JobRunAssigned, update *model.InstructionSet) error {
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
		JobId:                     jobId,
		State:                     pointer.Int32(int32(lookout.JobPendingOrdinal)),
		LastTransitionTime:        &ts,
		LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
		LatestRunId:               &runId,
	}

	update.JobsToUpdate = append(update.JobsToUpdate, &job)
	cluster := ""
	if len(event.GetResourceInfos()) > 0 {
		cluster = util.Truncate(event.GetResourceInfos()[0].GetObjectMeta().GetExecutorId(), maxClusterLen)
	}
	// Now create a job run
	jobRun := model.CreateJobRunInstruction{
		RunId:       runId,
		JobId:       jobId,
		Cluster:     cluster,
		Pending:     ts,
		JobRunState: lookout.JobRunPendingOrdinal,
	}
	update.JobRunsToCreate = append(update.JobRunsToCreate, &jobRun)
	return nil
}

func (c *InstructionConverter) handleJobRunSucceeded(ts time.Time, event *armadaevents.JobRunSucceeded, update *model.InstructionSet) error {
	runId, err := armadaevents.UuidStringFromProtoUuid(event.RunId)
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return errors.WithStack(err)
	}

	jobRun := model.UpdateJobRunInstruction{
		RunId:       runId,
		Finished:    &ts,
		JobRunState: pointer.Int32(lookout.JobRunSucceededOrdinal),
		ExitCode:    pointer.Int32(0),
	}
	update.JobRunsToUpdate = append(update.JobRunsToUpdate, &jobRun)
	return nil
}

func (c *InstructionConverter) handleJobRunErrors(ts time.Time, event *armadaevents.JobRunErrors, update *model.InstructionSet) error {
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
					jobRun.Cluster = util.Truncate(objectMeta.ExecutorId, maxClusterLen)
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
				jobRunUpdate.JobRunState = pointer.Int32(lookout.JobRunFailedOrdinal)
				jobRunUpdate.Error = tryCompressError(jobId, reason.PodError.GetMessage(), c.compressor)
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
				jobRunUpdate.JobRunState = pointer.Int32(lookout.JobRunTerminatedOrdinal)
				jobRunUpdate.Error = tryCompressError(jobId, reason.PodTerminated.GetMessage(), c.compressor)
			case *armadaevents.Error_PodUnschedulable:
				jobRunUpdate.Node = extractNodeName(reason.PodUnschedulable)
				jobRunUpdate.JobRunState = pointer.Int32(lookout.JobRunUnableToScheduleOrdinal)
				jobRunUpdate.Error = tryCompressError(jobId, reason.PodUnschedulable.GetMessage(), c.compressor)
			case *armadaevents.Error_PodLeaseReturned:
				jobRunUpdate.JobRunState = pointer.Int32(lookout.JobRunLeaseReturnedOrdinal)
				jobRunUpdate.Error = tryCompressError(jobId, reason.PodLeaseReturned.GetMessage(), c.compressor)
			case *armadaevents.Error_LeaseExpired:
				jobRunUpdate.JobRunState = pointer.Int32(lookout.JobRunLeaseExpiredOrdinal)
				jobRunUpdate.Error = tryCompressError(jobId, "Lease expired", c.compressor)
			default:
				jobRunUpdate.JobRunState = pointer.Int32(lookout.JobRunFailedOrdinal)
				jobRunUpdate.Error = tryCompressError(jobId, "Unknown error", c.compressor)
				log.Debugf("Ignoring event %T", reason)
			}
			update.JobRunsToUpdate = append(update.JobRunsToUpdate, jobRunUpdate)
			break
		}
	}
	return nil
}

func (c *InstructionConverter) handleJobRunPreempted(ts time.Time, event *armadaevents.JobRunPreempted, update *model.InstructionSet) error {
	jobId, err := armadaevents.UlidStringFromProtoUuid(event.PreemptedJobId)
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	runId, err := armadaevents.UuidStringFromProtoUuid(event.PreemptedRunId)
	if err != nil {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		return err
	}

	// Update Job
	job := model.UpdateJobInstruction{
		JobId:                     jobId,
		State:                     pointer.Int32(int32(lookout.JobPreemptedOrdinal)),
		LastTransitionTime:        &ts,
		LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
		LatestRunId:               &runId,
	}

	update.JobsToUpdate = append(update.JobsToUpdate, &job)

	// Update job run
	errorString := "preempted by non armada pod"
	preemptiveJobId, err := parseUlidString(event.PreemptiveJobId)
	if err != nil {
		log.WithError(err).Debug("failed to convert preemptive job id")
	} else {
		errorString = fmt.Sprintf("preempted by job %s", preemptiveJobId)
	}

	jobRun := model.UpdateJobRunInstruction{
		RunId:       runId,
		JobRunState: pointer.Int32(lookout.JobRunPreemptedOrdinal),
		Finished:    &ts,
		Error:       tryCompressError(jobId, errorString, c.compressor),
	}
	update.JobRunsToUpdate = append(update.JobRunsToUpdate, &jobRun)
	return nil
}

func parseUlidString(id *armadaevents.Uuid) (string, error) {
	if id == nil {
		return "", errors.New("uuid is nil")
	}
	// Likely wrong if it is zeroed
	if id.High64 == 0 && id.Low64 == 0 {
		return "", errors.New("")
	}
	stringId, err := armadaevents.UlidStringFromProtoUuid(id)
	if err != nil {
		return "", errors.Wrap(err, "could not convert non-nil preemptive job id")
	}
	return stringId, nil
}

func tryCompressError(jobId string, errorString string, compressor compress.Compressor) []byte {
	compressedError, err := compressor.Compress([]byte(errorString))
	if err != nil {
		log.WithError(err).Warnf("Couldn't compress error for job %s.", jobId)
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

func getNode(resources []*armadaevents.KubernetesResourceInfo) *string {
	for _, r := range resources {
		node := extractNodeName(r.GetPodInfo())
		if node != nil {
			return node
		}
	}
	return pointer.String("UNKNOWN")
}

func createFakeJobRun(jobId string, ts time.Time) *model.CreateJobRunInstruction {
	runId := uuid.New().String()
	return &model.CreateJobRunInstruction{
		RunId:       runId,
		JobId:       jobId,
		Cluster:     "UNKNOWN",
		Pending:     ts,
		JobRunState: lookout.JobRunPendingOrdinal,
	}
}

func extractNodeName(x HasNodeName) *string {
	nodeName := x.GetNodeName()
	if len(nodeName) > 0 {
		return pointer.String(util.Truncate(nodeName, maxNodeLen))
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
