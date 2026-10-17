package armadactl

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/domain"
)

// Watch prints events associated with a particular job set.
func (a *App) Watch(queue string, jobSetId string, raw bool, exitOnInactive bool, forceNewEvents bool, forceLegacyEvents bool) error {
	fmt.Fprintf(a.Out, "Watching job set %s\n", jobSetId)
	return client.WithEventClient(a.Params.ApiConnectionDetails, func(c api.EventClient) error {
		client.WatchJobSet(c, queue, jobSetId, true, true, forceNewEvents, forceLegacyEvents, armadacontext.Background(), func(state *domain.WatchContext, event api.Event) bool {
			if raw {
				data, err := json.Marshal(event)
				if err != nil {
					fmt.Fprintf(a.Out, "error parsing event %s: %s\n", event, err)
				} else {
					fmt.Fprintf(a.Out, "%s %s\n", reflect.TypeOf(event), string(data))
				}
			} else {
				switch event2 := event.(type) {
				case *api.JobUtilisationEvent:
					// no print
				case *api.JobFailedEvent:
					a.printSummary(state, event)
					fmt.Fprintf(a.Out, "Job failed: %s\n", event2.Reason)
					if hint := kubectlLogsHint(event2); hint != "" {
						fmt.Fprintf(a.Out, "To see the logs, try '%s --tail=50'\n", hint)
					}
				default:
					a.printSummary(state, event)
				}
			}
			if exitOnInactive && state.GetNumberOfJobs() == state.GetNumberOfFinishedJobs() {
				return true
			}
			return false
		})
		return nil
	})
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

// kubectlLogsHint returns a kubectl logs command for the pod of a failed run. It returns "" for a run without a pod.
func kubectlLogsHint(event *api.JobFailedEvent) string {
	if event.PodName == "" {
		return ""
	}
	return client.GetKubectlCommandForPod(event.ClusterId, event.PodNamespace, event.PodName, "logs")
}
