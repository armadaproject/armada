package cmd

import (
	"context"
	"encoding/json"
	"reflect"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"github.com/G-Research/armada/pkg/client/domain"
)

func init() {
	rootCmd.AddCommand(analyzeCmd)
}

// analyzeCmd represents the analyze command
var analyzeCmd = &cobra.Command{
	Use:   "analyze <queue> <jobSet>",
	Short: "Analyze job events in job set.",
	Long:  ``,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {

		queue := args[0]
		jobSetId := args[1]

		log.Infof("job set %s", jobSetId)

		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		client.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			eventsClient := api.NewEventClient(conn)

			events := map[string][]*api.Event{}
			var jobState *domain.WatchContext

			client.WatchJobSet(eventsClient, queue, jobSetId, false, context.Background(), func(state *domain.WatchContext, e api.Event) bool {
				events[e.GetJobId()] = append(events[e.GetJobId()], &e)
				jobState = state
				return false
			})

			if jobState == nil {
				log.Infof("No events found in jobset %s (queue: %s)", jobSetId, queue)
				return
			}

			for id, jobInfo := range jobState.GetCurrentState() {
				if jobInfo.Status != domain.Succeeded {
					jobEvents := events[id]

					log.Println()
					for _, e := range jobEvents {
						data, err := json.Marshal(e)
						if err != nil {
							log.Error(e)
						} else {
							log.Infof("%s %s\n", reflect.TypeOf(*e), string(data))
						}
					}
					log.Println()
				}
			}

		})
	},
}
