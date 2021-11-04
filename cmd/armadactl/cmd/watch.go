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

		client.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			eventsClient := api.NewEventClient(conn)
			client.WatchJobEvents(context.Background(), eventsClient, queue, jobSetId, func(state *domain.WatchContext, e api.Event) bool {
				if raw {
					data, err := json.Marshal(e)
					if err != nil {
						log.Error(e)
					} else {
						log.Infof("%s %s\n", reflect.TypeOf(e), string(data))
					}
				} else {

					switch event := e.(type) {
					case *api.JobUtilisationEvent:
						// no print
					case *api.JobFailedEvent:
						printSummary(state, e)
						log.Errorf("Failure reason:\n%s\n", event.Reason)

						jobInfo := state.GetJobInfo(event.JobId)
						if jobInfo != nil && jobInfo.ClusterId != "" && jobInfo.Job != nil {
							log.Errorf("You might be able to get the pod logs by running (logs are available for limited time):\n%s --tail=50\n",
								client.GetKubectlCommand(jobInfo.ClusterId, jobInfo.Job.Namespace, event.JobId, int(event.PodNumber), "logs"))
						}
					default:
						printSummary(state, e)
					}
				}
				if exit_on_inactive && state.GetNumberOfJobs() == state.GetNumberOfFinishedJobs() {
					return true
				}
				return false
			})
		})
	},
}

func printSummary(state *domain.WatchContext, e api.Event) {
	summary := fmt.Sprintf("%s | ", e.GetCreated().Format(time.Stamp))
	summary += state.GetCurrentStateSummary()
	summary += fmt.Sprintf(" | %s, job id: %s", reflect.TypeOf(e).String()[5:], e.GetJobId())

	if kubernetesEvent, ok := e.(api.KubernetesEvent); ok {
		summary += fmt.Sprintf(" pod: %d", kubernetesEvent.GetPodNumber())
	}
	log.Info(summary)
}
