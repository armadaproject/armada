package cmd

import (
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
}

// watchCmd represents the watch command
var watchCmd = &cobra.Command{
	Use:   "watch",
	Short: "Watch job events in job set.",
	Long:  `This command will list all job set events and `,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {

		jobSetId := args[0]

		log.Infof("Watching job set %s", jobSetId)

		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		util.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			eventsClient := api.NewEventClient(conn)
			service.WatchJobSet(eventsClient, jobSetId, func(state map[string]*service.JobInfo, e api.Event) {

				states := []service.JobStatus{
					service.Queued,
					service.Leased,
					service.Pending,
					service.Running,
					service.Succeeded,
					service.Failed,
					service.Cancelled}

				counts := service.CountStates(state)

				first := true

				fmt.Printf("%s | ", e.GetCreated().Format(time.Stamp))
				for _, state := range states {
					if !first {
						fmt.Print(", ")
					}
					first = false
					fmt.Printf("%s: %3d", state, counts[state])
				}
				fmt.Printf(" | event: %s, job id: %s", reflect.TypeOf(e), e.GetJobId())
				fmt.Println()
			})
		})
	},
}
