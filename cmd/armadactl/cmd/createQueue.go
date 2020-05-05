package cmd

import (
	"strconv"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

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
		queue := args[0]
		priority, _ := cmd.Flags().GetFloat64("priorityFactor")
		owners, _ := cmd.Flags().GetStringSlice("owners")
		groups, _ := cmd.Flags().GetStringSlice("groupOwners")
		resourceLimits, _ := cmd.Flags().GetStringToString("resourceLimits")
		resourceLimitsFloat, err := convertResourceLimitsToFloat64(resourceLimits)
		if err != nil {
			log.Error(err)
			return
		}

		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		client.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			submissionClient := api.NewSubmitClient(conn)
			e := client.CreateQueue(submissionClient, &api.Queue{
				Name:           queue,
				PriorityFactor: priority,
				UserOwners:     owners,
				GroupOwners:    groups,
				ResourceLimits: resourceLimitsFloat})

			if e != nil {
				log.Error(e)
				return
			}
			log.Infof("Queue %s created.", queue)
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
