package instructions

import (
	"time"

	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func handleJobRunRunning(ts time.Time, queue string, event *armadaevents.JobRunRunning) (Update, error) {
	return Update{
		Job: &JobRow{
			JobId:              event.JobId,
			Queue:              queue,
			JobState:           pointer.String(string(lookout.JobRunning)),
			RunState:           pointer.String(string(lookout.JobRunRunning)),
			RunStartedTs:       &ts,
			LastTransitionTime: &ts,
			LastUpdateTs:       ts,
		},
	}, nil
}

func handleJobRunLeased(ts time.Time, queue string, event *armadaevents.JobRunLeased) (Update, error) {
	return Update{
		Job: &JobRow{
			JobId:              event.JobId,
			Queue:              queue,
			JobState:           pointer.String(string(lookout.JobLeased)),
			LatestRunId:        &event.RunId,
			RunCluster:         &event.ExecutorId,
			RunState:           pointer.String(string(lookout.JobRunLeased)),
			RunNode:            &event.NodeId,
			RunLeasedTs:        &ts,
			LastTransitionTime: &ts,
			LastUpdateTs:       ts,
		},
		JobRun: &JobRunRow{
			JobId:    event.JobId,
			RunId:    event.RunId,
			Cluster:  &event.ExecutorId,
			State:    pointer.String(string(lookout.JobRunLeased)),
			Node:     &event.NodeId,
			LeasedTs: &ts,
			Merged:   pointer.Bool(true),
		},
	}, nil
}

func handleJobRunAssigned(ts time.Time, queue string, event *armadaevents.JobRunAssigned) (Update, error) {
	return Update{
		Job: &JobRow{
			JobId:              event.JobId,
			Queue:              queue,
			JobState:           pointer.String(string(lookout.JobPending)),
			RunState:           pointer.String(string(lookout.JobRunPending)),
			RunPendingTs:       &ts,
			LastTransitionTime: &ts,
			LastUpdateTs:       ts,
		},
		JobRun: &JobRunRow{
			JobId:     event.JobId,
			RunId:     event.RunId,
			PendingTs: &ts,
			State:     pointer.String(string(lookout.JobRunPending)),
		},
	}, nil
}

func handleJobRunCancelled(ts time.Time, queue string, event *armadaevents.JobRunCancelled) (Update, error) {
	return Update{
		Job: &JobRow{
			JobId:         event.JobId,
			Queue:         queue,
			RunState:      pointer.String(string(lookout.JobRunCancelled)),
			RunFinishedTs: &ts,
			LastUpdateTs:  ts,
		},
		JobRun: &JobRunRow{
			JobId:      event.JobId,
			RunId:      event.RunId,
			FinishedTS: &ts,
			State:      pointer.String(string(lookout.JobRunCancelled)),
		},
	}, nil
}

func handleJobRunSucceeded(ts time.Time, queue string, event *armadaevents.JobRunSucceeded) (Update, error) {
	return Update{
		Job: &JobRow{
			JobId:         event.JobId,
			Queue:         queue,
			RunState:      pointer.String(string(lookout.JobRunSucceeded)),
			RunFinishedTs: &ts,
			LastUpdateTs:  ts,
		},
		JobRun: &JobRunRow{
			JobId:      event.JobId,
			RunId:      event.RunId,
			FinishedTS: &ts,
			State:      pointer.String(string(lookout.JobRunSucceeded)),
		},
	}, nil
}

func handleJobRunErrors(ts time.Time, queue string, event *armadaevents.JobRunErrors) (Update, error) {
	for _, e := range event.GetErrors() {

		if !e.Terminal {
			continue
		}

		var exitCode int32 = 0
		var errorMsg string
		var debugMessage string
		var runState string

		switch reason := e.Reason.(type) {
		case *armadaevents.Error_PodError:
			for _, containerError := range reason.PodError.ContainerErrors {
				if containerError.ExitCode != 0 {
					exitCode = containerError.ExitCode
					break
				}
			}
			runState = string(lookout.JobRunFailed)
			errorMsg = reason.PodError.GetMessage()
			debugMessage = reason.PodError.GetDebugMessage()
		case *armadaevents.Error_PodLeaseReturned:
			runState = string(lookout.JobRunLeaseReturned)
			errorMsg = reason.PodLeaseReturned.GetMessage()
			debugMessage = reason.PodLeaseReturned.GetDebugMessage()
		case *armadaevents.Error_LeaseExpired:
			runState = string(lookout.JobRunLeaseExpired)
			errorMsg = "Lease Expired"
		case *armadaevents.Error_JobRunPreemptedError:
			// This case is already handled by the JobRunPreempted event
			// When we formalise that as a terminal event, we'll remove this JobRunError getting produced
			continue
		default:
			runState = string(lookout.JobRunFailed)
			errorMsg = "Unknown error"
		}
		return Update{
			Job: &JobRow{
				JobId:         event.JobId,
				Queue:         queue,
				RunState:      &runState,
				RunExitCode:   &exitCode,
				RunFinishedTs: &ts,
				LastUpdateTs:  ts,
			},
			JobRun: &JobRunRow{
				JobId:      event.JobId,
				RunId:      event.RunId,
				ExitCode:   &exitCode,
				State:      &runState,
				FinishedTS: &ts,
			},
			JobDebug: &JobDebugRow{
				RunId:        event.RunId,
				DebugMessage: debugMessage,
			},
			JobError: &JobErrorRow{
				RunId:        event.RunId,
				ErrorMessage: errorMsg,
			},
		}, nil
	}
	return Update{}, nil
}

func handleJobRunPreempted(ts time.Time, queue string, event *armadaevents.JobRunPreempted) (Update, error) {
	return Update{
		Job: &JobRow{
			Queue:         queue,
			JobId:         event.PreemptedRunId,
			RunState:      pointer.String(string(lookout.JobRunPreempted)),
			RunFinishedTs: &ts,
			LastUpdateTs:  ts,
		},
	}, nil
}
