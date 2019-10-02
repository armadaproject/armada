package cmd

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/client"
	"github.com/G-Research/k8s-batch/internal/client/util"
	"github.com/G-Research/k8s-batch/internal/common"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func init() {
	rootCmd.AddCommand(cancelCmd)
	createQueueCmd.Flags().String(
		"jobId", "", "job to cancel")
	createQueueCmd.Flags().String(
		"queue", "", "queue to cancel jobs from (requires job set to be specified)")
	createQueueCmd.Flags().String(
		"jobSet", "", "jobSet to cancel (requires queue to be specified)")
}

var cancelCmd = &cobra.Command{
	Use:   "cancel jobId",
	Short: "Cancels jobs in armada",
	Long:  `Cancels jobs either by jobId or by combination of queue & job set.`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		util.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			client := api.NewSubmitClient(conn)

			jobId, _ := cmd.Flags().GetString("jobId")
			queue, _ := cmd.Flags().GetString("queue")
			jobSet, _ := cmd.Flags().GetString("jobSet")

			ctx, cancel := common.ContextWithDefaultTimeout()
			defer cancel()
			_, e := client.CancelJob(ctx, &api.JobCancelRequest{
				JobId:    jobId,
				JobSetId: queue,
				Queue:    jobSet,
			})
			if e != nil {
				log.Error(e)
				return
			}
			log.Info("Cancellation request submitted.")
		})
	},
}
