package cmd

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/client"
	"github.com/G-Research/k8s-batch/internal/client/service"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func init() {
	rootCmd.AddCommand(createQueueCmd)
	createQueueCmd.Flags().Float64("priority", 1, "Set queue priority")
}

// createQueueCmd represents the createQueue command
var createQueueCmd = &cobra.Command{
	Use:   "create-queue name",
	Short: "Create new queue",
	Long: `Every job submitted to armada needs to be associated with queue. 
Job priority is evaluated inside queue, queue has its own priority.`,

	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		queue := args[0]
		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		service.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			client := api.NewSubmitClient(conn)
			_, e := client.CreateQueue(timeout(), &api.Queue{Name: queue, Priority: 1})

			if e != nil {
				log.Error(e)
				return
			}
			log.Infof("Queue %s created.", queue)
		})
	},
}
