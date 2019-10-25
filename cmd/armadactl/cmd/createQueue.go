package cmd

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/client"
	"github.com/G-Research/armada/internal/client/service"
	"github.com/G-Research/armada/internal/client/util"
)

func init() {
	rootCmd.AddCommand(createQueueCmd)
	createQueueCmd.Flags().Float64(
		"priorityFactor", 1,
		"Set queue priority factor - lower number makes queue more important, must be > 0.")
	createQueueCmd.Flags().StringArray(
		"owners", []string{},
		"Comma separated list of queue owners, defaults to current user.")
	createQueueCmd.Flags().StringArray(
		"groupOwners", []string{},
		"Comma separated list of queue group owners, defaults to empty list.")
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
		owners, _ := cmd.Flags().GetStringArray("owners")
		groups, _ := cmd.Flags().GetStringArray("groupOwners")

		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		util.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			client := api.NewSubmitClient(conn)
			e := service.CreateQueue(client, &api.Queue{
				Name:           queue,
				PriorityFactor: priority,
				UserOwners:     owners,
				GroupOwners:    groups})

			if e != nil {
				log.Error(e)
				return
			}
			log.Infof("Queue %s created.", queue)
		})
	},
}
