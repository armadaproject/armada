package armadactl

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/podname"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/domain"
)

// Watch prints events associated with a particular job set.
//
// If exitOnInactive is set, Watch returns once the job set is currently inactive, i.e. once all the events recorded so
// far have been printed and every job in the job set has finished. Moments of inactivity in the history of the job set
// do not stop the watch.
func (a *App) Watch(queue string, jobSetId string, raw bool, exitOnInactive bool, forceNewEvents bool, forceLegacyEvents bool) error {
	fmt.Fprintf(a.Out, "Watching job set %s\n", jobSetId)
	return client.WithEventClient(a.Params.ApiConnectionDetails, func(c api.EventClient) error {
		return a.watch(c, queue, jobSetId, raw, exitOnInactive, forceNewEvents, forceLegacyEvents)
	})
}

func (a *App) watch(c api.EventClient, queue string, jobSetId string, raw bool, exitOnInactive bool, forceNewEvents bool, forceLegacyEvents bool) error {
	if !exitOnInactive {
		client.WatchJobSet(c, queue, jobSetId, true, true, forceNewEvents, forceLegacyEvents, armadacontext.Background(), func(state *domain.WatchContext, event api.Event) bool {
			a.printEvent(state, event, raw)
			return false
		})
		return nil
	}

	// The event stream does not tell the client when it has caught up with the events that were recorded before the
	// watch started. So, to exit only if the job set is currently inactive, the history is read first, without
	// waiting for new events. If the job set is still active at the end of the history, the watch starts again and
	// skips over the events that were already printed. The watch can stop at a moment of inactivity that is not the
	// end of the history, as events may have been recorded since the history was read, so the history is read again
	// before exiting.
	printed := 0
	for {
		seen := 0
		state := client.WatchJobSet(c, queue, jobSetId, false, true, forceNewEvents, forceLegacyEvents, armadacontext.Background(), func(state *domain.WatchContext, event api.Event) bool {
			seen++
			if seen > printed {
				a.printEvent(state, event, raw)
				printed = seen
			}
			return false
		})
		if state.GetNumberOfJobs() > 0 && state.GetNumberOfJobs() == state.GetNumberOfFinishedJobs() {
			return nil
		}

		seen = 0
		becameInactive := false
		client.WatchJobSet(c, queue, jobSetId, true, true, forceNewEvents, forceLegacyEvents, armadacontext.Background(), func(state *domain.WatchContext, event api.Event) bool {
			seen++
			if seen <= printed {
				return false
			}
			a.printEvent(state, event, raw)
			printed = seen
			becameInactive = state.GetNumberOfJobs() == state.GetNumberOfFinishedJobs()
			return becameInactive
		})
		if !becameInactive {
			return nil
		}
	}
}

func (a *App) printEvent(state *domain.WatchContext, event api.Event, raw bool) {
	if raw {
		data, err := json.Marshal(event)
		if err != nil {
			fmt.Fprintf(a.Out, "error parsing event %s: %s\n", event, err)
		} else {
			fmt.Fprintf(a.Out, "%s %s\n", reflect.TypeOf(event), string(data))
		}
		return
	}
	switch event2 := event.(type) {
	case *api.JobUtilisationEvent:
		// no print
	case *api.JobFailedEvent:
		a.printSummary(state, event)
		fmt.Fprintf(a.Out, "Job failed: %s\n", event2.Reason)
		if hint := kubectlLogsHint(event2, state.GetJobInfo(event2.JobId)); hint != "" {
			fmt.Fprintf(a.Out, "To see the logs, try '%s --tail=50'\n", hint)
		}
	default:
		a.printSummary(state, event)
	}
}

func (a *App) printSummary(state *domain.WatchContext, e api.Event) {
	ts := protoutil.ToStdTime(e.GetCreated())
	summary := fmt.Sprintf("%s | ", ts.Format(time.Stamp))
	summary += state.GetCurrentStateSummary()
	summary += fmt.Sprintf(" | %s, job id: %s", reflect.TypeOf(e).String()[5:], e.GetJobId())
	if requestor := requestorFromEvent(e); requestor != "" {
		summary += fmt.Sprintf(", user: %s", requestor)
	}

	if kubernetesEvent, ok := e.(api.KubernetesEvent); ok {
		summary += fmt.Sprintf(" pod: %d", kubernetesEvent.GetPodNumber())
	}
	fmt.Fprintf(a.Out, "%s\n", summary)
}

func requestorFromEvent(e api.Event) string {
	switch event := e.(type) {
	case *api.JobCancellingEvent:
		return event.GetRequestor()
	case *api.JobCancelledEvent:
		return event.GetRequestor()
	case *api.JobReprioritizingEvent:
		return event.GetRequestor()
	case *api.JobReprioritizedEvent:
		return event.GetRequestor()
	case *api.JobPreemptingEvent:
		return event.GetRequestor()
	case *api.JobPreemptedEvent:
		return event.GetRequestor()
	default:
		return ""
	}
}

// kubectlLogsHint returns a kubectl logs command for the pod of a failed run. A failed event without a pod name gets
// the command for the job-scoped pod name, so that the hint stays the same for a server that does not send the name.
// It returns "" when the watch does not know the cluster of the job.
func kubectlLogsHint(event *api.JobFailedEvent, jobInfo *domain.JobInfo) string {
	if event.PodName != "" {
		return client.GetKubectlCommandForPod(event.ClusterId, event.PodNamespace, event.PodName, "logs")
	}
	if jobInfo != nil && jobInfo.ClusterId != "" && jobInfo.Job != nil {
		return client.GetKubectlCommandForPod(jobInfo.ClusterId, jobInfo.Job.Namespace, podname.Default(event.JobId), "logs")
	}
	return ""
}
