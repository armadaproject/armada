package armadactl

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"google.golang.org/grpc"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"github.com/G-Research/armada/pkg/client/domain"
)

func (a *App) Analyze(queue string, jobSetId string) error {
	fmt.Fprintf(a.Out, "Querying queue %s for job set %s\n", queue, jobSetId)
	client.WithConnection(a.Params.ApiConnectionDetails, func(conn *grpc.ClientConn) {
		eventsClient := api.NewEventClient(conn)

		events := map[string][]*api.Event{}
		var jobState *domain.WatchContext

		client.WatchJobSet(eventsClient, queue, jobSetId, false, true, context.Background(), func(state *domain.WatchContext, e api.Event) bool {
			events[e.GetJobId()] = append(events[e.GetJobId()], &e)
			jobState = state
			return false
		})

		if jobState == nil {
			fmt.Fprintf(a.Out, "Found no events associated with job set %s in queue %s/n", jobSetId, queue)
			return
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
	})
	return nil
}
