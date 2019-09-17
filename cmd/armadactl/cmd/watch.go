package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/client"
	"github.com/G-Research/k8s-batch/internal/client/service"
	"github.com/G-Research/k8s-batch/internal/client/util"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"reflect"
	"time"
)

func init() {
	rootCmd.AddCommand(watchCmd)
	watchCmd.Flags().Bool("raw", false, "Output raw events")
}

// watchCmd represents the watch command
var watchCmd = &cobra.Command{
	Use:   "watch",
	Short: "Watch job events in job set.",
	Long:  `This command will list all job set events and `,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {

		jobSetId := args[0]
		raw, _ := cmd.Flags().GetBool("raw")

		log.Infof("Watching job set %s", jobSetId)

		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		util.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			eventsClient := api.NewEventClient(conn)
			service.WatchJobSet(eventsClient, jobSetId, true, func(state map[string]*service.JobInfo, e api.Event) bool {
				if raw {
					data, err := json.Marshal(e)
					if err != nil {
						fmt.Print(e)
					} else {
						fmt.Printf("%s %s\n", reflect.TypeOf(e), string(data))
					}
				} else {
					fmt.Printf("%s | ", e.GetCreated().Format(time.Stamp))
					fmt.Print(service.CreateSummaryOfCurrentState(state))
					fmt.Printf(" | event: %s, job id: %s", reflect.TypeOf(e), e.GetJobId())
					fmt.Println()
				}
				return false
			})
		})
	},
}
