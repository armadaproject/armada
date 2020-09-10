package cmd

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

func init() {
	rootCmd.AddCommand(deleteQueueCmd)
}

var deleteQueueCmd = &cobra.Command{
	Use:   "delete-queue name",
	Short: "Delete existing queue",
	Long:  `This commands removes queue if it exists, the queue needs to be empty at the time of deletion.`,

	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		queue := args[0]

		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		client.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			submissionClient := api.NewSubmitClient(conn)
			e := client.DeleteQueue(submissionClient, queue)
			if e != nil {
				exitWithError(e)
			}
			log.Infof("Queue %s deleted or did not exist.", queue)
		})
	},
}
