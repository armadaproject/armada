package cmd

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"time"
)

// createQueueCmd represents the createQueue command
var createQueueCmd = &cobra.Command{
	Use:   "createQueue",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		queue := args[0]

		url := cmd.Flag("armadaUrl").Value.String()
		conn, err := grpc.Dial(url, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.WaitForReady(false)))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		defer conn.Close()

		client := api.NewSubmitClient(conn)
		background := context.Background()
		ctx, _ := context.WithTimeout(background, 30*time.Second)
		_, e := client.CreateQueue(ctx, &api.Queue{Name: queue, Priority: 1})

		if e != nil {
			log.Error(e)
			return
		}
		log.Infof("Queue %s created.", queue)
	},
}

func init() {
	rootCmd.AddCommand(createQueueCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// createQueueCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// createQueueCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
