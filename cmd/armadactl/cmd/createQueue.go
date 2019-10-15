package cmd

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/client"
	"github.com/G-Research/k8s-batch/internal/client/service"
	"github.com/G-Research/k8s-batch/internal/client/util"
)

func init() {
	rootCmd.AddCommand(createQueueCmd)
	createQueueCmd.Flags().Float64(
		"priorityFactor", 1,
		"Set queue priority factor - lower number makes queue more important, must be > 0.")
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
		priority, _ := cmd.Flags().GetFloat64("priorityFactor")

		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		util.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			client := api.NewSubmitClient(conn)
			e := service.CreateQueue(client, &api.Queue{Name: queue, PriorityFactor: priority})

			if e != nil {
				log.Error(e)
				return
			}
			log.Infof("Queue %s created.", queue)
		})
	},
}
