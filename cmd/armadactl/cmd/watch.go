package cmd

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/client"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

// watchCmd represents the watch command
var watchCmd = &cobra.Command{
	Use:   "watch",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {

		jobSetId := args[0]

		url := cmd.Flag("armadaUrl").Value.String()
		conn, err := grpc.Dial(url, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.WaitForReady(false)))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		defer conn.Close()

		log.Infof("Watching job set %s", jobSetId)

		eventsClient := api.NewEventClient(conn)
		client.WatchJobSet(eventsClient, jobSetId)
	},
}

func init() {
	rootCmd.AddCommand(watchCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// watchCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// watchCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
