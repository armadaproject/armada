package instructions

import (
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/model"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	maxQueueLen         = 512
	maxOwnerLen         = 512
	maxJobSetLen        = 1024
	maxAnnotationKeyLen = 1024
	maxAnnotationValLen = 1024
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
	return &InstructionConverter{
		metrics:              m,
		userAnnotationPrefix: userAnnotationPrefix,
		compressor:           compressor,
	}
}

func (c *InstructionConverter) Convert(ctx *armadacontext.Context, sequences *utils.EventsWithIds[*armadaevents.EventSequence]) *model.InstructionSet {
	updateInstructions := &model.InstructionSet{
		MessageIds: sequences.MessageIds,
	}

	for _, es := range sequences.Events {
		c.convertSequence(ctx, es, updateInstructions)
	}
	return updateInstructions
}

func (c *InstructionConverter) convertSequence(
	ctx *armadacontext.Context,
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
		ts := protoutil.ToStdTime(event.Created)
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
		case *armadaevents.EventSequence_Event_JobRunCancelled:
			err = c.handleJobRunCancelled(ts, event.GetJobRunCancelled(), update)
		case *armadaevents.EventSequence_Event_JobRunSucceeded:
			err = c.handleJobRunSucceeded(ts, event.GetJobRunSucceeded(), update)
		case *armadaevents.EventSequence_Event_JobRunErrors:
			err = c.handleJobRunErrors(ts, event.GetJobRunErrors(), update)
		case *armadaevents.EventSequence_Event_JobRunPreempted:
			err = c.handleJobRunPreempted(ts, event.GetJobRunPreempted(), update)
		case *armadaevents.EventSequence_Event_JobRequeued:
			err = c.handleJobRequeued(ts, event.GetJobRequeued(), update)
		case *armadaevents.EventSequence_Event_JobRunLeased:
			err = c.handleJobRunLeased(ts, event.GetJobRunLeased(), update)
		case *armadaevents.EventSequence_Event_ReprioritiseJobSet:
		case *armadaevents.EventSequence_Event_CancelJob:
		case *armadaevents.EventSequence_Event_CancelJobSet:
		case *armadaevents.EventSequence_Event_ResourceUtilisation:
		case *armadaevents.EventSequence_Event_StandaloneIngressInfo:
		case *armadaevents.EventSequence_Event_PartitionMarker:
		case *armadaevents.EventSequence_Event_JobValidated:
			log.Debugf("Ignoring event type %T", event.GetEvent())
		default:
			log.Warnf("Ignoring unknown event type %T", event.GetEvent())
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
	// Try and marshall the job proto. This shouldn't go wrong but if it does, it's not a fatal error
	// Rather it means that the job spec won't be available in the ui
	var jobProto []byte
	apiJob, err := eventutil.ApiJobFromLogSubmitJob(owner, []string{}, queue, jobSet, ts, event)
	if err == nil {

		jobProtoUncompressed, err := proto.Marshal(apiJob)
		if err != nil {
			log.WithError(err).Warnf("Couldn't marshall job %s in jobset %s as proto.", event.JobId, jobSet)
		}

		jobProto, err = c.compressor.Compress(jobProtoUncompressed)
		if err != nil {
			log.WithError(err).Warnf("Couldn't compress proto for job %s in jobset %s.", event.JobId, jobSet)
		}
	} else {
		c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
		log.WithError(err).Warnf("Couldn't convert job event for job %s in jobset %s to api job.", event.JobId, jobSet)
	}

	resources := getJobResources(apiJob)
	priorityClass := getJobPriorityClass(apiJob)
	if priorityClass != nil {
		truncatedPriorityClass := util.Truncate(*priorityClass, maxPriorityClassLen)
		priorityClass = &truncatedPriorityClass
	}

	annotations := event.GetObjectMeta().GetAnnotations()
	userAnnotations := extractUserAnnotations(c.userAnnotationPrefix, annotations)
	externalJobUri := util.Truncate(annotations["armadaproject.io/externalJobUri"], maxAnnotationValLen)

	job := model.CreateJobInstruction{
		JobId:                     event.JobId,
		Queue:                     queue,
		Owner:                     owner,
		Namespace:                 apiJob.Namespace,
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
		Annotations:               userAnnotations,
		ExternalJobUri:            externalJobUri,
	}
	update.JobsToCreate = append(update.JobsToCreate, &job)

	return err
}

// extractUserAnnotations strips userAnnotationPrefix from all keys and truncates keys and values to their maximal
// lengths (as specified by maxAnnotationKeyLen and maxAnnotationValLen).
func extractUserAnnotations(userAnnotationPrefix string, jobAnnotations map[string]string) map[string]string {
	result := make(map[string]string, len(jobAnnotations))
	n := len(userAnnotationPrefix)
	for k, v := range jobAnnotations {
		if strings.HasPrefix(k, userAnnotationPrefix) {
			k = k[n:]
		}
		k = util.Truncate(k, maxAnnotationKeyLen)
		v = util.Truncate(v, maxAnnotationValLen)
		result[k] = v
	}
	return result
}

func (c *InstructionConverter) handleReprioritiseJob(ts time.Time, event *armadaevents.ReprioritisedJob, update *model.InstructionSet) error {
	jobUpdate := model.UpdateJobInstruction{
		JobId:    event.JobId,
		Priority: pointer.Int64(int64(event.Priority)),
	}
	update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	return nil
}

func (c *InstructionConverter) handleCancelledJob(ts time.Time, event *armadaevents.CancelledJob, update *model.InstructionSet) error {
	var reason *string
	if event.Reason != "" {
		reason = &event.Reason
	}
	jobUpdate := model.UpdateJobInstruction{
		JobId:                     event.GetJobId(),
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
	jobUpdate := model.UpdateJobInstruction{
		JobId:                     event.JobId,
		State:                     pointer.Int32(int32(lookout.JobSucceededOrdinal)),
		LastTransitionTime:        &ts,
		LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
	}
	update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	return nil
}

func (c *InstructionConverter) handleJobErrors(ts time.Time, event *armadaevents.JobErrors, update *model.InstructionSet) error {
	for _, e := range event.GetErrors() {
		if !e.Terminal {
			continue
		}

		state := lookout.JobFailedOrdinal
		switch reason := e.Reason.(type) {
		// Preempted and Rejected jobs are modelled as Reasons on a JobErrors msg
		case *armadaevents.Error_JobRunPreemptedError:
			state = lookout.JobPreemptedOrdinal
		case *armadaevents.Error_JobRejected:
			state = lookout.JobRejectedOrdinal
			update.JobErrorsToCreate = append(update.JobErrorsToCreate, &model.CreateJobErrorInstruction{
				JobId: event.JobId,
				Error: tryCompressError(event.JobId, reason.JobRejected.Message, c.compressor),
			})
		}

		jobUpdate := model.UpdateJobInstruction{
			JobId:                     event.JobId,
			State:                     pointer.Int32(int32(state)),
			LastTransitionTime:        &ts,
			LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
		}
		update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
		break
	}

	return nil
}

func (c *InstructionConverter) handleJobRunRunning(ts time.Time, event *armadaevents.JobRunRunning, update *model.InstructionSet) error {
	// Update Job
	job := model.UpdateJobInstruction{
		JobId:                     event.JobId,
		State:                     pointer.Int32(int32(lookout.JobRunningOrdinal)),
		LastTransitionTime:        &ts,
		LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
		LatestRunId:               &event.RunId,
	}

	update.JobsToUpdate = append(update.JobsToUpdate, &job)

	// Update Job Run
	node := getNode(event.ResourceInfos)
	jobRun := model.UpdateJobRunInstruction{
		RunId:       event.RunId,
		Node:        node,
		Started:     &ts,
		JobRunState: pointer.Int32(lookout.JobRunRunningOrdinal),
	}
	update.JobRunsToUpdate = append(update.JobRunsToUpdate, &jobRun)
	return nil
}

func (c *InstructionConverter) handleJobRequeued(ts time.Time, event *armadaevents.JobRequeued, update *model.InstructionSet) error {
	jobUpdate := model.UpdateJobInstruction{
		JobId:                     event.JobId,
		State:                     pointer.Int32(int32(lookout.JobQueuedOrdinal)),
		LastTransitionTime:        &ts,
		LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
	}
	update.JobsToUpdate = append(update.JobsToUpdate, &jobUpdate)
	return nil
}

func (c *InstructionConverter) handleJobRunLeased(ts time.Time, event *armadaevents.JobRunLeased, update *model.InstructionSet) error {
	// Update Job
	job := model.UpdateJobInstruction{
		JobId:                     event.JobId,
		State:                     pointer.Int32(int32(lookout.JobLeasedOrdinal)),
		LastTransitionTime:        &ts,
		LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
		LatestRunId:               &event.RunId,
	}

	update.JobsToUpdate = append(update.JobsToUpdate, &job)
	// Now create a job run
	jobRun := model.CreateJobRunInstruction{
		RunId:       event.RunId,
		JobId:       event.JobId,
		Cluster:     util.Truncate(event.ExecutorId, maxClusterLen),
		Node:        pointer.String(util.Truncate(event.NodeId, maxNodeLen)),
		Leased:      &ts,
		JobRunState: lookout.JobRunLeasedOrdinal,
	}
	update.JobRunsToCreate = append(update.JobRunsToCreate, &jobRun)
	return nil
}

func (c *InstructionConverter) handleJobRunAssigned(ts time.Time, event *armadaevents.JobRunAssigned, update *model.InstructionSet) error {
	// Update Job
	job := model.UpdateJobInstruction{
		JobId:                     event.JobId,
		State:                     pointer.Int32(int32(lookout.JobPendingOrdinal)),
		LastTransitionTime:        &ts,
		LastTransitionTimeSeconds: pointer.Int64(ts.Unix()),
		LatestRunId:               &event.RunId,
	}

	update.JobsToUpdate = append(update.JobsToUpdate, &job)
	jobRun := model.UpdateJobRunInstruction{
		RunId:       event.RunId,
		Pending:     &ts,
		JobRunState: pointer.Int32(lookout.JobRunPendingOrdinal),
	}
	update.JobRunsToUpdate = append(update.JobRunsToUpdate, &jobRun)
	return nil
}

func (c *InstructionConverter) handleJobRunCancelled(ts time.Time, event *armadaevents.JobRunCancelled, update *model.InstructionSet) error {
	jobRun := model.UpdateJobRunInstruction{
		RunId:       event.RunId,
		Finished:    &ts,
		JobRunState: pointer.Int32(lookout.JobRunCancelledOrdinal),
	}
	update.JobRunsToUpdate = append(update.JobRunsToUpdate, &jobRun)
	return nil
}

func (c *InstructionConverter) handleJobRunSucceeded(ts time.Time, event *armadaevents.JobRunSucceeded, update *model.InstructionSet) error {
	jobRun := model.UpdateJobRunInstruction{
		RunId:       event.RunId,
		Finished:    &ts,
		JobRunState: pointer.Int32(lookout.JobRunSucceededOrdinal),
		ExitCode:    pointer.Int32(0),
	}
	update.JobRunsToUpdate = append(update.JobRunsToUpdate, &jobRun)
	return nil
}

func (c *InstructionConverter) handleJobRunErrors(ts time.Time, event *armadaevents.JobRunErrors, update *model.InstructionSet) error {
	for _, e := range event.GetErrors() {
		jobRunUpdate := &model.UpdateJobRunInstruction{
			RunId: event.RunId,
		}
		if e.Terminal {
			jobRunUpdate.Finished = &ts
		}

		switch reason := e.Reason.(type) {
		case *armadaevents.Error_PodError:
			jobRunUpdate.Node = extractNodeName(reason.PodError)
			jobRunUpdate.JobRunState = pointer.Int32(lookout.JobRunFailedOrdinal)
			jobRunUpdate.Error = tryCompressError(event.JobId, reason.PodError.GetMessage(), c.compressor)
			jobRunUpdate.Debug = tryCompressError(event.JobId, reason.PodError.DebugMessage, c.compressor)
			var exitCode int32 = 0
			for _, containerError := range reason.PodError.ContainerErrors {
				if containerError.ExitCode != 0 {
					exitCode = containerError.ExitCode
					break
				}
			}
			jobRunUpdate.ExitCode = pointer.Int32(exitCode)
		case *armadaevents.Error_JobRunPreemptedError:
			// This case is already handled by the JobRunPreempted event
			// When we formalise that as a terminal event, we'll remove this JobRunError getting produced
			continue
		case *armadaevents.Error_PodUnschedulable:
			jobRunUpdate.Node = extractNodeName(reason.PodUnschedulable)
		case *armadaevents.Error_PodLeaseReturned:
			jobRunUpdate.JobRunState = pointer.Int32(lookout.JobRunLeaseReturnedOrdinal)
			jobRunUpdate.Error = tryCompressError(event.JobId, reason.PodLeaseReturned.GetMessage(), c.compressor)
			jobRunUpdate.Debug = tryCompressError(event.JobId, reason.PodLeaseReturned.GetDebugMessage(), c.compressor)
		case *armadaevents.Error_LeaseExpired:
			jobRunUpdate.JobRunState = pointer.Int32(lookout.JobRunLeaseExpiredOrdinal)
			jobRunUpdate.Error = tryCompressError(event.JobId, "Lease expired", c.compressor)
		default:
			jobRunUpdate.JobRunState = pointer.Int32(lookout.JobRunFailedOrdinal)
			jobRunUpdate.Error = tryCompressError(event.JobId, "Unknown error", c.compressor)
			log.Debugf("Ignoring event %T", reason)
		}
		update.JobRunsToUpdate = append(update.JobRunsToUpdate, jobRunUpdate)
		break
	}
	return nil
}

func (c *InstructionConverter) handleJobRunPreempted(ts time.Time, event *armadaevents.JobRunPreempted, update *model.InstructionSet) error {
	jobRun := model.UpdateJobRunInstruction{
		RunId:       event.PreemptedRunId,
		JobRunState: pointer.Int32(lookout.JobRunPreemptedOrdinal),
		Finished:    &ts,
		Error:       tryCompressError(event.PreemptedJobId, "preempted", c.compressor),
	}
	update.JobRunsToUpdate = append(update.JobRunsToUpdate, &jobRun)
	return nil
}

func tryCompressError(jobId string, errorString string, compressor compress.Compressor) []byte {
	compressedError, err := compressor.Compress([]byte(errorString))
	if err != nil {
		log.WithError(err).Warnf("Couldn't compress error for job %s.", jobId)
	}
	return compressedError
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

func extractNodeName(x HasNodeName) *string {
	nodeName := x.GetNodeName()
	if len(nodeName) > 0 {
		return pointer.String(util.Truncate(nodeName, maxNodeLen))
	}
	return nil
}

func getJobResources(job *api.Job) jobResources {
	resources := jobResources{}

	podSpec := job.GetMainPodSpec()

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
	podSpec := job.GetMainPodSpec()
	if podSpec.PriorityClassName != "" {
		return pointer.String(podSpec.PriorityClassName)
	}
	return nil
}
