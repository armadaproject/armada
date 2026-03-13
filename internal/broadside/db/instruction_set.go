package db

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common/database/lookout"
	lookoutmodel "github.com/armadaproject/armada/internal/lookoutingester/model"
)

// queriesToInstructionSet converts a batch of IngestionQuery values into a
// lookoutmodel.InstructionSet suitable for passing to the LookoutDb methods.
//
// Multiple job or job-run updates for the same ID within a batch are conflated
// (last write wins per field) to avoid undefined behaviour in the UPDATE … FROM
// temp-table pattern when duplicate source rows are present.
func queriesToInstructionSet(queries []IngestionQuery) (*lookoutmodel.InstructionSet, error) {
	jobCreates := make(map[string]*lookoutmodel.CreateJobInstruction)

	// Keyed by JobID / RunID for last-write-wins conflation.
	jobUpdates := make(map[string]*lookoutmodel.UpdateJobInstruction)
	jobRunUpdates := make(map[string]*lookoutmodel.UpdateJobRunInstruction)

	set := &lookoutmodel.InstructionSet{}

	for _, query := range queries {
		switch q := query.(type) {
		case InsertJob:
			instr := jobToCreateInstruction(q)
			jobCreates[q.Job.JobID] = instr

		case InsertJobRun:
			set.JobRunsToCreate = append(set.JobRunsToCreate, jobRunToCreateInstruction(q))

		case InsertJobError:
			set.JobErrorsToCreate = append(set.JobErrorsToCreate, &lookoutmodel.CreateJobErrorInstruction{
				JobId: q.JobID,
				Error: q.Error,
			})

		case UpdateJobPriority:
			u := jobUpdate(jobUpdates, q.JobID)
			u.Priority = &q.Priority

		case SetJobLeased:
			state := int32(lookout.JobLeasedOrdinal)
			seconds := q.Time.Unix()
			u := jobUpdate(jobUpdates, q.JobID)
			u.State = &state
			u.LatestRunId = &q.RunID
			u.LastTransitionTime = &q.Time
			u.LastTransitionTimeSeconds = &seconds

		case SetJobPending:
			state := int32(lookout.JobPendingOrdinal)
			seconds := q.Time.Unix()
			u := jobUpdate(jobUpdates, q.JobID)
			u.State = &state
			u.LatestRunId = &q.RunID
			u.LastTransitionTime = &q.Time
			u.LastTransitionTimeSeconds = &seconds

		case SetJobRunning:
			state := int32(lookout.JobRunningOrdinal)
			seconds := q.Time.Unix()
			u := jobUpdate(jobUpdates, q.JobID)
			u.State = &state
			u.LatestRunId = &q.LatestRunID
			u.LastTransitionTime = &q.Time
			u.LastTransitionTimeSeconds = &seconds

		case SetJobErrored:
			state := int32(lookout.JobFailedOrdinal)
			seconds := q.Time.Unix()
			u := jobUpdate(jobUpdates, q.JobID)
			u.State = &state
			u.LastTransitionTime = &q.Time
			u.LastTransitionTimeSeconds = &seconds

		case SetJobSucceeded:
			state := int32(lookout.JobSucceededOrdinal)
			seconds := q.Time.Unix()
			u := jobUpdate(jobUpdates, q.JobID)
			u.State = &state
			u.LastTransitionTime = &q.Time
			u.LastTransitionTimeSeconds = &seconds

		case SetJobCancelled:
			state := int32(lookout.JobCancelledOrdinal)
			seconds := q.Time.Unix()
			u := jobUpdate(jobUpdates, q.JobID)
			u.State = &state
			u.Cancelled = &q.Time
			u.LastTransitionTime = &q.Time
			u.LastTransitionTimeSeconds = &seconds

		case SetJobRejected:
			state := int32(lookout.JobRejectedOrdinal)
			seconds := q.Time.Unix()
			u := jobUpdate(jobUpdates, q.JobID)
			u.State = &state
			u.LastTransitionTime = &q.Time
			u.LastTransitionTimeSeconds = &seconds

		case SetJobPreempted:
			state := int32(lookout.JobPreemptedOrdinal)
			seconds := q.Time.Unix()
			u := jobUpdate(jobUpdates, q.JobID)
			u.State = &state
			u.LastTransitionTime = &q.Time
			u.LastTransitionTimeSeconds = &seconds

		case SetJobRunPending:
			state := int32(lookout.JobRunPendingOrdinal)
			u := runUpdate(jobRunUpdates, q.JobRunID)
			u.Pending = &q.Time
			u.JobRunState = &state

		case SetJobRunStarted:
			state := int32(lookout.JobRunRunningOrdinal)
			u := runUpdate(jobRunUpdates, q.JobRunID)
			u.Node = &q.Node
			u.Started = &q.Time
			u.JobRunState = &state

		case SetJobRunSucceeded:
			state := int32(lookout.JobRunSucceededOrdinal)
			u := runUpdate(jobRunUpdates, q.JobRunID)
			u.Finished = &q.Time
			u.ExitCode = &q.ExitCode
			u.JobRunState = &state

		case SetJobRunFailed:
			state := int32(lookout.JobRunFailedOrdinal)
			u := runUpdate(jobRunUpdates, q.JobRunID)
			u.Finished = &q.Time
			u.Error = q.Error
			u.Debug = q.Debug
			u.ExitCode = &q.ExitCode
			u.JobRunState = &state

		case SetJobRunCancelled:
			state := int32(lookout.JobRunCancelledOrdinal)
			u := runUpdate(jobRunUpdates, q.JobRunID)
			u.Finished = &q.Time
			u.JobRunState = &state

		case SetJobRunPreempted:
			state := int32(lookout.JobRunPreemptedOrdinal)
			u := runUpdate(jobRunUpdates, q.JobRunID)
			u.Finished = &q.Time
			u.Error = q.Error
			u.JobRunState = &state

		default:
			return nil, fmt.Errorf("unknown ingestion query type: %T", query)
		}
	}

	for _, instr := range jobCreates {
		set.JobsToCreate = append(set.JobsToCreate, instr)
	}
	for _, instr := range jobUpdates {
		set.JobsToUpdate = append(set.JobsToUpdate, instr)
	}
	for _, instr := range jobRunUpdates {
		set.JobRunsToUpdate = append(set.JobRunsToUpdate, instr)
	}

	return set, nil
}

// jobUpdate returns the existing UpdateJobInstruction for jobID, creating one
// if absent. This implements last-write-wins conflation within a batch.
func jobUpdate(m map[string]*lookoutmodel.UpdateJobInstruction, jobID string) *lookoutmodel.UpdateJobInstruction {
	u, ok := m[jobID]
	if !ok {
		u = &lookoutmodel.UpdateJobInstruction{JobId: jobID}
		m[jobID] = u
	}
	return u
}

// runUpdate returns the existing UpdateJobRunInstruction for runID, creating
// one if absent.
func runUpdate(m map[string]*lookoutmodel.UpdateJobRunInstruction, runID string) *lookoutmodel.UpdateJobRunInstruction {
	u, ok := m[runID]
	if !ok {
		u = &lookoutmodel.UpdateJobRunInstruction{RunId: runID}
		m[runID] = u
	}
	return u
}

func jobToCreateInstruction(q InsertJob) *lookoutmodel.CreateJobInstruction {
	job := q.Job
	var priorityClass *string
	if job.PriorityClass != "" {
		priorityClass = &job.PriorityClass
	}
	return &lookoutmodel.CreateJobInstruction{
		JobId:                     job.JobID,
		Queue:                     job.Queue,
		Owner:                     job.Owner,
		Namespace:                 job.Namespace,
		JobSet:                    job.JobSet,
		Cpu:                       job.Cpu,
		Memory:                    job.Memory,
		EphemeralStorage:          job.EphemeralStorage,
		Gpu:                       job.Gpu,
		Priority:                  job.Priority,
		Submitted:                 job.Submitted,
		State:                     lookout.JobQueuedOrdinal,
		LastTransitionTime:        job.Submitted,
		LastTransitionTimeSeconds: job.Submitted.Unix(),
		PriorityClass:             priorityClass,
		Annotations:               job.Annotations,
		JobProto:                  q.JobSpec,
	}
}

func jobRunToCreateInstruction(q InsertJobRun) *lookoutmodel.CreateJobRunInstruction {
	leased := q.Time
	instr := &lookoutmodel.CreateJobRunInstruction{
		RunId:       q.JobRunID,
		JobId:       q.JobID,
		Cluster:     q.Cluster,
		Leased:      &leased,
		JobRunState: lookout.JobRunLeasedOrdinal,
	}
	if q.Node != "" {
		instr.Node = &q.Node
	}
	if q.Pool != "" {
		instr.Pool = q.Pool
	}
	return instr
}
