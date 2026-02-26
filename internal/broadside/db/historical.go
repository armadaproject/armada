package db

import (
	"fmt"
	"time"

	"github.com/armadaproject/armada/internal/broadside/jobspec"
)

const (
	historicalJobAge           = 24 * time.Hour
	historicalLeasedOffset     = time.Second
	historicalPendingOffset    = 2 * time.Second
	historicalRunningOffset    = 3 * time.Second
	historicalTerminatedOffset = 10 * time.Second
)

func historicalJobID(queueIdx, jobSetIdx, jobNum int) string {
	return fmt.Sprintf("%04d%04d%010d", queueIdx, jobSetIdx, jobNum)
}

func historicalState(jobNum int, p HistoricalJobsParams) jobspec.JobState {
	pos := jobNum % 1000
	switch {
	case pos < p.SucceededThreshold:
		return jobspec.StateSucceeded
	case pos < p.ErroredThreshold:
		return jobspec.StateErrored
	case pos < p.CancelledThreshold:
		return jobspec.StateCancelled
	default:
		return jobspec.StatePreempted
	}
}

func buildHistoricalJobQueries(jobNum int, params HistoricalJobsParams) []IngestionQuery {
	baseTime := time.Now().Add(-historicalJobAge)
	jobID := historicalJobID(params.QueueIdx, params.JobSetIdx, jobNum)
	runID := jobspec.EncodeRunID(jobID, 0)
	cluster, node := jobspec.GetClusterNodeForJobNumber(jobNum)

	leasedTime := baseTime.Add(historicalLeasedOffset)
	pendingTime := baseTime.Add(historicalPendingOffset)
	runningTime := baseTime.Add(historicalRunningOffset)
	terminalTime := baseTime.Add(historicalTerminatedOffset)

	newJob := &NewJob{
		JobID:            jobID,
		Queue:            params.QueueName,
		JobSet:           params.JobSetName,
		Owner:            params.QueueName,
		Namespace:        jobspec.GetNamespace(jobNum),
		Priority:         int64((jobNum % jobspec.PriorityValues) + 1),
		PriorityClass:    jobspec.GetPriorityClass(jobNum),
		Submitted:        baseTime,
		Cpu:              jobspec.GetCpu(jobNum),
		Memory:           jobspec.GetMemory(jobNum),
		EphemeralStorage: jobspec.GetEphemeralStorage(jobNum),
		Gpu:              jobspec.GetGpu(jobNum),
		Annotations:      jobspec.GenerateAnnotationsForJob(jobNum),
	}

	queries := []IngestionQuery{
		InsertJob{Job: newJob},
		InsertJobSpec{JobID: jobID, JobSpec: string(params.JobSpecBytes)},
		SetJobLeased{JobID: jobID, Time: leasedTime, RunID: runID},
		InsertJobRun{JobRunID: runID, JobID: jobID, Cluster: cluster, Node: node, Pool: jobspec.GetPool(jobNum), Time: leasedTime},
		SetJobPending{JobID: jobID, Time: pendingTime, RunID: runID},
		SetJobRunPending{JobRunID: runID, Time: pendingTime},
		SetJobRunning{JobID: jobID, Time: runningTime, LatestRunID: runID},
		SetJobRunStarted{JobRunID: runID, Time: runningTime, Node: node},
	}

	switch historicalState(jobNum, params) {
	case jobspec.StateSucceeded:
		queries = append(queries,
			SetJobSucceeded{JobID: jobID, Time: terminalTime},
			SetJobRunSucceeded{JobRunID: runID, Time: terminalTime},
		)
	case jobspec.StateErrored:
		queries = append(queries,
			SetJobErrored{JobID: jobID, Time: terminalTime},
			SetJobRunFailed{JobRunID: runID, Time: terminalTime, Error: params.ErrorBytes, Debug: params.DebugBytes},
			InsertJobError{JobID: jobID, Error: params.ErrorBytes},
		)
	case jobspec.StateCancelled:
		queries = append(queries,
			SetJobCancelled{JobID: jobID, Time: terminalTime, CancelReason: "user requested", CancelUser: params.QueueName},
			SetJobRunCancelled{JobRunID: runID, Time: terminalTime},
		)
	case jobspec.StatePreempted:
		queries = append(queries,
			SetJobPreempted{JobID: jobID, Time: terminalTime},
			SetJobRunPreempted{JobRunID: runID, Time: terminalTime, Error: params.PreemptionBytes},
		)
	}

	return queries
}
