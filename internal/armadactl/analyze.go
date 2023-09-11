package armadactl

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/domain"
)

func (a *App) Analyze(queue string, jobSetId string) error {
	fmt.Fprintf(a.Out, "Querying queue %s for job set %s\n", queue, jobSetId)
	return client.WithEventClient(a.Params.ApiConnectionDetails, func(ec api.EventClient) error {
		events := map[string][]*api.Event{}
		var jobState *domain.WatchContext

		client.WatchJobSet(ec, queue, jobSetId, false, true, false, false, armadacontext.Background(), func(state *domain.WatchContext, e api.Event) bool {
			events[e.GetJobId()] = append(events[e.GetJobId()], &e)
			jobState = state
			return false
		})

		if jobState == nil {
			fmt.Fprintf(a.Out, "Found no events associated with job set %s in queue %s/n", jobSetId, queue)
			return nil
		}

		for id, jobInfo := range jobState.GetCurrentState() {
			if jobInfo.Status != domain.Succeeded {
				jobEvents := events[id]

				fmt.Fprintf(a.Out, "\n")
				for _, e := range jobEvents {
					data, err := json.Marshal(e)
					if err != nil {
						fmt.Fprintf(a.Out, "Error marshalling JSON: %s\n", err)
					} else {
						fmt.Fprintf(a.Out, "%s %s\n", reflect.TypeOf(*e), string(data))
					}
				}
				fmt.Fprintf(a.Out, "\n")
			}
		}
		return nil
	})
}
