package armadactl

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"github.com/G-Research/armada/pkg/client/domain"
)

// Watch prints events associated with a particular job set.
func (a *App) Watch(queue string, jobSetId string, raw bool, exit_on_inactive bool) error {
	fmt.Fprintf(a.Out, "Watching job set %s\n", jobSetId)
	return client.WithEventClient(a.Params.ApiConnectionDetails, func(c api.EventClient) error {
		client.WatchJobSet(c, queue, jobSetId, true, true, context.Background(), func(state *domain.WatchContext, event api.Event) bool {
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

					jobInfo := state.GetJobInfo(event2.JobId)
					if jobInfo != nil && jobInfo.ClusterId != "" && jobInfo.Job != nil {
						fmt.Fprintf(
							a.Out, "Found no logs for job; try '%s --tail=50\n",
							client.GetKubectlCommand(jobInfo.ClusterId, jobInfo.Job.Namespace, event2.JobId, int(event2.PodNumber), "logs"),
						)
					}
				default:
					a.printSummary(state, event)
				}
			}
			if exit_on_inactive && state.GetNumberOfJobs() == state.GetNumberOfFinishedJobs() {
				return true
			}
			return false
		})
		return nil
	})
}

func (a *App) printSummary(state *domain.WatchContext, e api.Event) {
	summary := fmt.Sprintf("%s | ", e.GetCreated().Format(time.Stamp))
	summary += state.GetCurrentStateSummary()
	summary += fmt.Sprintf(" | %s, job id: %s", reflect.TypeOf(e).String()[5:], e.GetJobId())

	if kubernetesEvent, ok := e.(api.KubernetesEvent); ok {
		summary += fmt.Sprintf(" pod: %d", kubernetesEvent.GetPodNumber())
	}
	fmt.Fprintf(a.Out, "%s\n", summary)
}
