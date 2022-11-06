package instructions

import (
	"github.com/G-Research/armada/internal/common/ingest"
	"github.com/G-Research/armada/internal/common/ingest/metrics"
	"github.com/G-Research/armada/internal/scheduler"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/G-Research/armada/internal/scheduleringester/model"
	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type InstructionCenverter struct {
	metrics *metrics.Metrics
}

func NewInstructionConverter(metrics *metrics.Metrics) ingest.InstructionConverter[*model.DbOperationsWithMessageIds] {
	return &InstructionCenverter{
		metrics: metrics,
	}
}

func (c *InstructionCenverter) Convert(sequencesWithIds *ingest.EventSequencesWithIds) *model.DbOperationsWithMessageIds {
	var operations = make([]model.DbOperation, 0)
	for _, es := range sequencesWithIds.EventSequences {
		for _, op := range c.convertSequence(es) {
			operations = model.AppendDbOperation(operations, op)
		}
	}
	return &model.DbOperationsWithMessageIds{
		Ops:        operations,
		MessageIds: sequencesWithIds.MessageIds,
	}
}

func (c *InstructionCenverter) convertSequence(es *armadaevents.EventSequence) []model.DbOperation {
	queue := es.Queue
	jobset := es.JobSetName
	user := es.UserId
	groups := es.Groups
	var operations = make([]model.DbOperation, 0, len(es.Events))
	for idx, event := range es.Events {
		var err error = nil
		var operationsFromEvent []model.DbOperation
		switch event.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_SubmitJob:
			operationsFromEvent, err = c.handleSubmitJob(queue, user, groups, jobset, event.GetSubmitJob())
		default:
			log.Warnf("Ignoring unknown event type %T", event)
		}
		if err != nil {
			c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
			log.WithError(err).Warnf("Could not convert event at index %d.", idx)
		} else {
			operations = append(operations, operationsFromEvent...)
		}
	}
	return operations
}

func (c *InstructionCenverter) handleSubmitJob(queue string, user string, groups []string, jobset string, job *armadaevents.SubmitJob) ([]model.DbOperation, error) {

	// Store the job submit message so that it can be sent to an executor.
	submitJobBytes, err := proto.Marshal(job)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Produce a minimal representation of the job for the scheduler.
	// To avoid the scheduler needing to load the entire job spec.
	schedulingInfo, err := schedulingInfoFromSubmitJob(job)
	if err != nil {
		return nil, err
	}
	schedulingInfoBytes, err := proto.Marshal(schedulingInfo)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	jobId := armadaevents.UuidFromProtoUuid(job.JobId)
	return []model.DbOperation{model.InsertJobs{jobId: &scheduler.Job{
		JobID:          jobId,
		JobSet:         jobset,
		UserID:         user,
		Groups:         groups,
		Queue:          queue,
		Priority:       int64(job.Priority),
		SubmitMessage:  submitJobBytes,
		SchedulingInfo: schedulingInfoBytes,
	}}}, nil
}

// schedulingInfoFromSubmitJob returns a minimal representation of a job
// containing only the info needed by the scheduler.
func schedulingInfoFromSubmitJob(submitJob *armadaevents.SubmitJob) (*schedulerobjects.JobSchedulingInfo, error) {
	// Component common to all jobs.
	schedulingInfo := &schedulerobjects.JobSchedulingInfo{
		Lifetime:        submitJob.Lifetime,
		AtMostOnce:      submitJob.AtMostOnce,
		Preemptible:     submitJob.Preemptible,
		ConcurrencySafe: submitJob.ConcurrencySafe,
	}

	// Scheduling requirements specific to the objects that make up this job.
	switch object := submitJob.MainObject.Object.(type) {
	case *armadaevents.KubernetesMainObject_PodSpec:
		podSpec := object.PodSpec.PodSpec
		requirements := &schedulerobjects.ObjectRequirements_PodRequirements{
			// TODO: We should not pass in nil here. Priority will not be set correctly.
			PodRequirements: schedulerobjects.PodRequirementsFromPodSpec(podSpec, nil),
		}
		schedulingInfo.ObjectRequirements = append(
			schedulingInfo.ObjectRequirements,
			&schedulerobjects.ObjectRequirements{Requirements: requirements},
		)
	default:
		return nil, errors.Errorf("unsupported object type %T", object)
	}
	return schedulingInfo, nil
}
