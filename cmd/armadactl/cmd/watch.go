package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"github.com/G-Research/armada/pkg/client/domain"
	"github.com/G-Research/armada/pkg/client/util"
)

func init() {
	rootCmd.AddCommand(watchCmd)
	watchCmd.Flags().Bool("raw", false, "Output raw events")
	watchCmd.Flags().Bool("exit-if-inactive", false, "Exit if there are no more active jobs")
}

// watchCmd represents the watch command
var watchCmd = &cobra.Command{
	Use:   "watch queue jobSet",
	Short: "Watch job events in job set.",
	Long:  `This command will list all job set events and `,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {

		queue := args[0]
		jobSetId := args[1]

		raw, _ := cmd.Flags().GetBool("raw")
		exit_on_inactive, _ := cmd.Flags().GetBool("exit-if-inactive")
		log.Infof("Watching job set %s", jobSetId)

		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		util.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			eventsClient := api.NewEventClient(conn)
			client.WatchJobSet(eventsClient, queue, jobSetId, true, context.Background(), func(state *domain.WatchContext, e api.Event) bool {
				if raw {
					data, err := json.Marshal(e)
					if err != nil {
						log.Error(e)
					} else {
						log.Infof("%s %s\n", reflect.TypeOf(e), string(data))
					}
				} else {
					summary := fmt.Sprintf("%s | ", e.GetCreated().Format(time.Stamp))
					summary += state.GetCurrentStateSummary()
					summary += fmt.Sprintf(" | event: %s, job id: %s", reflect.TypeOf(e), e.GetJobId())
					log.Info(summary)
				}
				if exit_on_inactive && state.GetNumberOfJobs() == state.GetNumberOfFinishedJobs() {
					return true
				}
				return false
			})
		})
	},
}
