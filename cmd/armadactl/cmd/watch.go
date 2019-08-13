package cmd

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/client"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
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

		withConnection(func(conn *grpc.ClientConn) {
			eventsClient := api.NewEventClient(conn)
			client.WatchJobSet(eventsClient, jobSetId)
		})
	},
}
