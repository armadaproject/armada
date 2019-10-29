package cmd

import (
	"context"
	"encoding/json"
	"reflect"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/client"
	"github.com/G-Research/armada/internal/client/service"
	"github.com/G-Research/armada/internal/client/util"
)

func init() {
	rootCmd.AddCommand(analyzeCmd)
}

// analyzeCmd represents the analyze command
var analyzeCmd = &cobra.Command{
	Use:   "analyze",
	Short: "Analyze job events in job set.",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {

		jobSetId := args[0]

		log.Infof("job set %s", jobSetId)

		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		util.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			eventsClient := api.NewEventClient(conn)

			events := map[string][]*api.Event{}
			jobState := map[string]*service.JobInfo{}

			service.WatchJobSet(eventsClient, jobSetId, false, context.Background(), func(state map[string]*service.JobInfo, e api.Event) bool {
				events[e.GetJobId()] = append(events[e.GetJobId()], &e)
				jobState = state
				return false
			})

			for id, jobInfo := range jobState {
				if jobInfo.Status != service.Succeeded {
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
