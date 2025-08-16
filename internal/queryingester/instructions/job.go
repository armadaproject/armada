package instructions

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/eventutil"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func handleSubmitJob(
	queue,
	owner,
	jobSet string,
	userAnnotationPrefix string,
	ts time.Time,
	event *armadaevents.SubmitJob,
) (Update, error) {
	apiJob, err := eventutil.ApiJobFromLogSubmitJob(owner, []string{}, queue, jobSet, ts, event)
	if err != nil {
		return Update{}, err
	}
	jobProto, err := proto.Marshal(apiJob)
	if err != nil {
		return Update{}, err
	}
	resources := getJobResources(apiJob)
	priorityClass := apiJob.GetMainPodSpec().PriorityClassName

	annotations := event.GetObjectMeta().GetAnnotations()
	userAnnotations := armadamaps.MapKeys(annotations, func(k string) string {
		return strings.TrimPrefix(k, userAnnotationPrefix)
	})

	annotationsJson, err := json.Marshal(userAnnotations)
	if err != nil {
		return Update{}, err
	}

	return Update{
		JobSpec: &JobSpecRow{
			JobId:   event.JobId,
			JobSpec: string(jobProto),
		},
		Job: &JobRow{
			JobId:              event.JobId,
			Queue:              queue,
			Namespace:          &apiJob.Namespace,
			JobSet:             &jobSet,
			Cpu:                &resources.Cpu,
			Memory:             &resources.Memory,
			EphemeralStorage:   &resources.EphemeralStorage,
			Gpu:                &resources.Gpu,
			Priority:           pointer.Int64(int64(event.Priority)),
			SubmitTs:           &ts,
			PriorityClass:      &priorityClass,
			Annotations:        pointer.String(string(annotationsJson)),
			JobState:           pointer.String(string(lookout.JobQueued)),
			LastTransitionTime: &ts,
			LastUpdateTs:       ts,
		},
	}, nil
}

func handleJobRequeued(ts time.Time, queue string, event *armadaevents.JobRequeued) (Update, error) {
	return Update{
		Job: &JobRow{
			JobId:              event.JobId,
			Queue:              queue,
			JobState:           pointer.String(string(lookout.JobQueued)),
			LastTransitionTime: &ts,
			LastUpdateTs:       ts,
		},
	}, nil
}

func handleReprioritiseJob(ts time.Time, queue string, event *armadaevents.ReprioritisedJob) (Update, error) {
	return Update{
		Job: &JobRow{
			JobId:        event.JobId,
			Queue:        queue,
			Priority:     pointer.Int64(int64(event.Priority)),
			LastUpdateTs: ts,
		},
	}, nil
}

func handleJobSucceeded(ts time.Time, queue string, event *armadaevents.JobSucceeded) (Update, error) {
	return Update{
		Job: &JobRow{
			JobId:              event.JobId,
			Queue:              queue,
			JobState:           pointer.String(string(lookout.JobSucceeded)),
			RunFinishedTs:      &ts,
			LastTransitionTime: &ts,
			LastUpdateTs:       ts,
		},
	}, nil
}

func handleCancelledJob(ts time.Time, queue string, event *armadaevents.CancelledJob) (Update, error) {
	return Update{
		Job: &JobRow{
			JobId:              event.JobId,
			Queue:              queue,
			JobState:           pointer.String(string(lookout.JobCancelled)),
			CancelTs:           &ts,
			CancelReason:       &event.Reason,
			CancelUser:         &event.CancelUser,
			LastTransitionTime: &ts,
			LastUpdateTs:       ts,
		},
	}, nil
}

func handleJobErrors(ts time.Time, queue string, event *armadaevents.JobErrors) (Update, error) {
	for _, e := range event.GetErrors() {
		// We don't care about non-terminal errors
		if !e.Terminal {
			continue
		}

		errMsg := ""
		state := lookout.JobFailed
		switch reason := e.Reason.(type) {
		// Preempted and Rejected jobs are modelled as Reasons on a JobErrors msg
		case *armadaevents.Error_JobRunPreemptedError:
			state = lookout.JobPreempted
		case *armadaevents.Error_JobRejected:
			state = lookout.JobRejected
			errMsg = reason.JobRejected.String()
		}

		return Update{
			Job: &JobRow{
				JobId:              event.JobId,
				Queue:              queue,
				JobState:           pointer.String(string(state)),
				LastTransitionTime: &ts,
				LastUpdateTs:       ts,
				Error:              pointer.String(errMsg),
			},
		}, nil
	}
	return Update{}, nil
}
