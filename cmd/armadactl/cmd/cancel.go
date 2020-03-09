package cmd

import (
	"strings"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"github.com/G-Research/armada/pkg/client/util"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func init() {
	rootCmd.AddCommand(cancelCmd)
	cancelCmd.Flags().String(
		"jobId", "", "job to cancel")
	cancelCmd.Flags().String(
		"queue", "", "queue to cancel jobs from (requires job set to be specified)")
	cancelCmd.Flags().String(
		"jobSet", "", "jobSet to cancel (requires queue to be specified)")
}

var cancelCmd = &cobra.Command{
	Use:   "cancel",
	Short: "Cancels jobs in armada",
	Long:  `Cancels jobs either by jobId or by combination of queue & job set.`,
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		util.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			client := api.NewSubmitClient(conn)

			jobId, _ := cmd.Flags().GetString("jobId")
			queue, _ := cmd.Flags().GetString("queue")
			jobSet, _ := cmd.Flags().GetString("jobSet")

			ctx, cancel := common.ContextWithDefaultTimeout()
			defer cancel()
			result, e := client.CancelJobs(ctx, &api.JobCancelRequest{
				JobId:    jobId,
				JobSetId: jobSet,
				Queue:    queue,
			})
			if e != nil {
				log.Error(e)
				return
			}
			log.Infof("Cancellation request submitted for jobs: %s", strings.Join(result.CancelledIds, ", "))
		})
	},
}
