package instructions

import (
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/clickhouseingester/model"
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
) (model.Update, error) {
	apiJob, err := eventutil.ApiJobFromLogSubmitJob(owner, []string{}, queue, jobSet, ts, event)
	if err != nil {
		return model.Update{}, err
	}
	jobProto, err := proto.Marshal(apiJob)
	if err != nil {
		return model.Update{}, err
	}
	resources := getJobResources(apiJob)
	priorityClass := apiJob.GetMainPodSpec().PriorityClassName

	annotations := event.GetObjectMeta().GetAnnotations()
	userAnnotations := armadamaps.MapKeys(annotations, func(k string) string {
		return strings.TrimPrefix(k, userAnnotationPrefix)
	})

	return model.Update{
		JobSpec: &model.JobSpecRow{
			JobId:   event.JobId,
			JobSpec: string(jobProto),
		},
		Job: &model.JobRow{
			JobId:              event.JobId,
			Queue:              &queue,
			Namespace:          &apiJob.Namespace,
			JobSet:             &jobSet,
			Cpu:                &resources.Cpu,
			Memory:             &resources.Memory,
			EphemeralStorage:   &resources.EphemeralStorage,
			Gpu:                &resources.Gpu,
			Priority:           pointer.Int64(int64(event.Priority)),
			SubmitTs:           &ts,
			PriorityClass:      &priorityClass,
			Annotations:        userAnnotations,
			JobState:           pointer.String(string(lookout.JobQueued)),
			LastTransitionTime: &ts,
			LastUpdateTs:       ts,
			Merged:             pointer.Bool(true),
		},
	}, nil
}

func handleJobRequeued(ts time.Time, event *armadaevents.JobRequeued) (model.Update, error) {
	return model.Update{
		Job: &model.JobRow{
			JobId:              event.JobId,
			JobState:           pointer.String(string(lookout.JobQueued)),
			LastTransitionTime: &ts,
			LastUpdateTs:       ts,
		},
	}, nil
}

func handleReprioritiseJob(ts time.Time, event *armadaevents.ReprioritisedJob) (model.Update, error) {
	return model.Update{
		Job: &model.JobRow{
			JobId:        event.JobId,
			Priority:     pointer.Int64(int64(event.Priority)),
			LastUpdateTs: ts,
		},
	}, nil
}

func handleJobSucceeded(ts time.Time, event *armadaevents.JobSucceeded) (model.Update, error) {
	return model.Update{
		Job: &model.JobRow{
			JobId:              event.JobId,
			JobState:           pointer.String(string(lookout.JobSucceeded)),
			RunFinishedTs:      &ts,
			LastTransitionTime: &ts,
			LastUpdateTs:       ts,
		},
	}, nil
}

func handleCancelledJob(ts time.Time, event *armadaevents.CancelledJob) (model.Update, error) {
	return model.Update{
		Job: &model.JobRow{
			JobId:              event.JobId,
			JobState:           pointer.String(string(lookout.JobCancelled)),
			CancelTs:           &ts,
			CancelReason:       &event.Reason,
			CancelUser:         &event.CancelUser,
			LastTransitionTime: &ts,
			LastUpdateTs:       ts,
		},
	}, nil
}

func handleJobErrors(ts time.Time, event *armadaevents.JobErrors) (model.Update, error) {
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

		return model.Update{
			Job: &model.JobRow{
				JobId:              event.JobId,
				JobState:           pointer.String(string(state)),
				LastTransitionTime: &ts,
				LastUpdateTs:       ts,
				Error:              pointer.String(errMsg),
			},
		}, nil
	}
	return model.Update{}, nil
}
