package cmd

import (
	"context"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

func init() {
	rootCmd.AddCommand(resourcesCmd)
	kubeCmd.FParseErrWhitelist.UnknownFlags = true
}

var resourcesCmd = &cobra.Command{
	Use:   "resources",
	Short: "Prints out maximum resource usage for individual jobs.",
	Long:  `Prints out maximum resource usage for individual jobs in job set.`,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		queue := args[0]
		jobSetId := args[1]

		client.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {

			eventsClient := api.NewEventClient(conn)
			state := client.GetJobSetState(eventsClient, queue, jobSetId, context.Background())

			for _, j := range state.GetCurrentState() {
				log.Infof("job id: %v, maximum used resources: %v", j.Job.Id, j.MaxUsedResources)
			}
		})
	},
}
