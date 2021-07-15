package cmd

import (
	"strconv"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

func init() {
	rootCmd.AddCommand(createQueueCmd)
	createQueueCmd.Flags().Float64(
		"priorityFactor", 1,
		"Set queue priority factor - lower number makes queue more important, must be > 0.")
	createQueueCmd.Flags().StringSlice(
		"owners", []string{},
		"Comma separated list of queue owners, defaults to current user.")
	createQueueCmd.Flags().StringSlice(
		"groupOwners", []string{},
		"Comma separated list of queue group owners, defaults to empty list.")
	createQueueCmd.Flags().StringToString(
		"resourceLimits", map[string]string{},
		"Command separated list of resource limits pairs, defaults to empty list. Example: --resourceLimits cpu=0.3,memory=0.2")
}

// createQueueCmd represents the createQueue command
var createQueueCmd = &cobra.Command{
	Use:   "create-queue name",
	Short: "Create new queue",
	Long: `Every job submitted to armada needs to be associated with queue. 
Job priority is evaluated inside queue, queue has its own priority.`,

	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		queueName := args[0]
		priority, _ := cmd.Flags().GetFloat64("priorityFactor")
		owners, _ := cmd.Flags().GetStringSlice("owners")
		groups, _ := cmd.Flags().GetStringSlice("groupOwners")
		resourceLimits, _ := cmd.Flags().GetStringToString("resourceLimits")
		resourceLimitsFloat, err := convertResourceLimitsToFloat64(resourceLimits)
		if err != nil {
			exitWithError(err)
		}

		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		client.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			submissionClient := api.NewSubmitClient(conn)

			queue := &api.Queue{
				Name:           queueName,
				PriorityFactor: priority,
				UserOwners:     owners,
				GroupOwners:    groups,
				ResourceLimits: resourceLimitsFloat}

			err = client.CreateQueue(submissionClient, queue)

			// Tempoary workaround to keep old "upsert" behavior
			// Long term plan is to add new "create queue", "replace queue" and "edit queue" commands, then deprecate "create-queue"
			if status.Code(err) == codes.AlreadyExists {
				log.Infof("Queue already exists, so OVERWRITING ALL SETTINGS of existing queue %s.", queue.Name)
				err = client.UpdateQueue(submissionClient, queue)
			}

			if err != nil {
				exitWithError(err)
			}
			log.Infof("Queue %s created/updated.", queue.Name)
		})
	},
}

func convertResourceLimitsToFloat64(resourceLimits map[string]string) (map[string]float64, error) {
	resourceLimitsFloat := make(map[string]float64, len(resourceLimits))
	for resourceName, limit := range resourceLimits {
		limitFloat, err := strconv.ParseFloat(limit, 64)
		if err != nil {
			return nil, err
		}
		resourceLimitsFloat[resourceName] = limitFloat
	}

	return resourceLimitsFloat, nil
}
