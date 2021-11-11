package cmd

import (
	"context"
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

func init() {
	rootCmd.AddCommand(infoCmd)
}

var infoCmd = &cobra.Command{
	Use:        "info <queue>",
	Short:      "Prints out queue info including all jobs sets where jobs are running or queued.",
	Long:       `Prints out queue info including all jobs sets where jobs are running or queued.`,
	Deprecated: `Use: "describe queue"`,
	Args:       cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		queue := args[0]

		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		client.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			submitClient := api.NewSubmitClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			queueInfo, e := submitClient.GetQueueInfo(ctx, &api.QueueInfoRequest{queue})
			if e != nil {
				exitWithError(e)
			}

			jobSets := queueInfo.ActiveJobSets
			sort.Slice(jobSets, func(i, j int) bool {
				return jobSets[i].Name < jobSets[j].Name
			})

			log.Infof("Queue %s:", queueInfo.Name)
			if len(jobSets) == 0 {
				log.Info("No job queued or running.")
			}

			for _, jobSet := range jobSets {
				log.Infof("in cluster: %d, queued: %d - %s", jobSet.LeasedJobs, jobSet.QueuedJobs, jobSet.Name)
			}
		})
	},
}
