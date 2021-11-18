package queue

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/spf13/cobra"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

func Describe() *cobra.Command {
	command := &cobra.Command{
		Use:   "queue <queueName>",
		Short: "Prints out queue info.",
		Long:  "Prints out queue info including all jobs sets where jobs are running or queued.",
		Args:  validateQueueName,
	}

	command.RunE = func(cmd *cobra.Command, args []string) error {
		queueName := args[0]

		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()
		conn, err := client.CreateApiConnection(apiConnectionDetails)
		if err != nil {
			return fmt.Errorf("failed to connect to api because %s", err)
		}
		defer conn.Close()

		apiClient := api.NewSubmitClient(conn)
		ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
		defer cancel()

		queueInfo, e := apiClient.GetQueueInfo(ctx, &api.QueueInfoRequest{Name: queueName})
		if e != nil {
			return fmt.Errorf("failed to retrieve queue info: %s", err)
		}

		jobSets := queueInfo.ActiveJobSets
		sort.SliceStable(jobSets, func(i, j int) bool {
			return jobSets[i].Name < jobSets[j].Name
		})

		cmd.Printf("Queue %s:", queueInfo.Name)
		if len(jobSets) == 0 {
			cmd.Printf("No job queued or running.")
			return nil
		}

		for _, jobSet := range jobSets {
			cmd.Printf("in cluster: %d, queued: %d - %s", jobSet.LeasedJobs, jobSet.QueuedJobs, jobSet.Name)
		}
		return nil
	}

	return command
}
