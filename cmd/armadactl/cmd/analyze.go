package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/client"
	"github.com/G-Research/k8s-batch/internal/client/service"
	"github.com/G-Research/k8s-batch/internal/client/util"
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

					fmt.Println()
					for _, e := range jobEvents {
						data, err := json.Marshal(e)
						if err != nil {
							fmt.Print(e)
						} else {
							fmt.Printf("%s %s\n", reflect.TypeOf(*e), string(data))
						}
					}
					fmt.Println()
				}
			}

		})
	},
}
