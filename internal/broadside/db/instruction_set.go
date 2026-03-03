package db

import (
	"fmt"

	lookoutmodel "github.com/armadaproject/armada/internal/lookoutingester/model"
)

// queriesToInstructionSet converts a batch of IngestionQuery values into a
// lookoutmodel.InstructionSet suitable for passing to the LookoutDb methods.
//
// InsertJob and InsertJobSpec are matched by JobID and merged into a single
// CreateJobInstruction. If InsertJobSpec is absent for a job in this batch,
// JobProto will be nil — callers must filter before passing to CreateJobSpecs.
//
// Multiple job or job-run updates for the same ID within a batch are conflated
// (last write wins per field) to avoid undefined behaviour in the UPDATE … FROM
// temp-table pattern when duplicate source rows are present.
func queriesToInstructionSet(queries []IngestionQuery) (*lookoutmodel.InstructionSet, error) {
	// Keyed by JobID so InsertJob and InsertJobSpec can be merged.
	jobCreates := make(map[string]*lookoutmodel.CreateJobInstruction)

	// Keyed by JobID / RunID for last-write-wins conflation.
	jobUpdates := make(map[string]*lookoutmodel.UpdateJobInstruction)
	jobRunUpdates := make(map[string]*lookoutmodel.UpdateJobRunInstruction)

	set := &lookoutmodel.InstructionSet{}

	for _, query := range queries {
		switch q := query.(type) {
		case InsertJob:
			instr, ok := jobCreates[q.Job.JobID]
			if !ok {
				instr = jobToCreateInstruction(q.Job)
				jobCreates[q.Job.JobID] = instr
			}
			_ = instr // spec bytes set by a later InsertJobSpec if present

		case InsertJobSpec:
			instr, ok := jobCreates[q.JobID]
			if !ok {
				instr = &lookoutmodel.CreateJobInstruction{JobId: q.JobID}
				jobCreates[q.JobID] = instr
			}
			instr.JobProto = []byte(q.JobSpec)

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
			state := int32(1)
			seconds := q.Time.Unix()
			u := jobUpdate(jobUpdates, q.JobID)
			u.State = &state
			u.LatestRunId = &q.RunID
			u.LastTransitionTime = &q.Time
			u.LastTransitionTimeSeconds = &seconds

		case SetJobPending:
			state := int32(2)
			seconds := q.Time.Unix()
			u := jobUpdate(jobUpdates, q.JobID)
			u.State = &state
			u.LatestRunId = &q.RunID
			u.LastTransitionTime = &q.Time
			u.LastTransitionTimeSeconds = &seconds

		case SetJobRunning:
			state := int32(3)
			seconds := q.Time.Unix()
			u := jobUpdate(jobUpdates, q.JobID)
			u.State = &state
			u.LatestRunId = &q.LatestRunID
			u.LastTransitionTime = &q.Time
			u.LastTransitionTimeSeconds = &seconds

		case SetJobErrored:
			state := int32(4)
			seconds := q.Time.Unix()
			u := jobUpdate(jobUpdates, q.JobID)
			u.State = &state
			u.LastTransitionTime = &q.Time
			u.LastTransitionTimeSeconds = &seconds

		case SetJobSucceeded:
			state := int32(5)
			seconds := q.Time.Unix()
			u := jobUpdate(jobUpdates, q.JobID)
			u.State = &state
			u.LastTransitionTime = &q.Time
			u.LastTransitionTimeSeconds = &seconds

		case SetJobCancelled:
			state := int32(6)
			seconds := q.Time.Unix()
			u := jobUpdate(jobUpdates, q.JobID)
			u.State = &state
			u.Cancelled = &q.Time
			u.LastTransitionTime = &q.Time
			u.LastTransitionTimeSeconds = &seconds

		case SetJobRejected:
			state := int32(7)
			seconds := q.Time.Unix()
			u := jobUpdate(jobUpdates, q.JobID)
			u.State = &state
			u.LastTransitionTime = &q.Time
			u.LastTransitionTimeSeconds = &seconds

		case SetJobPreempted:
			state := int32(8)
			seconds := q.Time.Unix()
			u := jobUpdate(jobUpdates, q.JobID)
			u.State = &state
			u.LastTransitionTime = &q.Time
			u.LastTransitionTimeSeconds = &seconds

		case SetJobRunPending:
			state := int32(1)
			u := runUpdate(jobRunUpdates, q.JobRunID)
			u.Pending = &q.Time
			u.JobRunState = &state

		case SetJobRunStarted:
			state := int32(2)
			u := runUpdate(jobRunUpdates, q.JobRunID)
			u.Node = &q.Node
			u.Started = &q.Time
			u.JobRunState = &state

		case SetJobRunSucceeded:
			state := int32(3)
			u := runUpdate(jobRunUpdates, q.JobRunID)
			u.Finished = &q.Time
			u.ExitCode = &q.ExitCode
			u.JobRunState = &state

		case SetJobRunFailed:
			state := int32(4)
			u := runUpdate(jobRunUpdates, q.JobRunID)
			u.Finished = &q.Time
			u.Error = q.Error
			u.Debug = q.Debug
			u.ExitCode = &q.ExitCode
			u.JobRunState = &state

		case SetJobRunCancelled:
			state := int32(6)
			u := runUpdate(jobRunUpdates, q.JobRunID)
			u.Finished = &q.Time
			u.JobRunState = &state

		case SetJobRunPreempted:
			state := int32(5)
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

// jobSpecInstructions returns only those instructions that have a non-nil
// JobProto. This filters out jobs whose InsertJobSpec arrived in a different
// batch from their InsertJob.
func jobSpecInstructions(jobs []*lookoutmodel.CreateJobInstruction) []*lookoutmodel.CreateJobInstruction {
	var result []*lookoutmodel.CreateJobInstruction
	for _, j := range jobs {
		if j.JobProto != nil {
			result = append(result, j)
		}
	}
	return result
}

func jobToCreateInstruction(job *NewJob) *lookoutmodel.CreateJobInstruction {
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
		State:                     0, // queued
		LastTransitionTime:        job.Submitted,
		LastTransitionTimeSeconds: job.Submitted.Unix(),
		PriorityClass:             priorityClass,
		Annotations:               job.Annotations,
	}
}

func jobRunToCreateInstruction(q InsertJobRun) *lookoutmodel.CreateJobRunInstruction {
	leased := q.Time
	instr := &lookoutmodel.CreateJobRunInstruction{
		RunId:       q.JobRunID,
		JobId:       q.JobID,
		Cluster:     q.Cluster,
		Leased:      &leased,
		JobRunState: 0, // leased
	}
	if q.Node != "" {
		instr.Node = &q.Node
	}
	if q.Pool != "" {
		instr.Pool = q.Pool
	}
	return instr
}
