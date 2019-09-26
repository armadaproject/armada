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
}

var cancelCmd = &cobra.Command{
	Use:   "cancel jobId",
	Short: "Cancels jobs in armada",
	Long: `

`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		jobId := args[0]

		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		util.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			client := api.NewSubmitClient(conn)

			ctx, cancel := common.ContextWithDefaultTimeout()
			defer cancel()
			_, e := client.CancelJob(ctx, &api.JobCancelRequest{JobId: jobId})
			if e != nil {
				log.Error(e)
				return
			}
			log.Infof("Submitted job id: %s ", jobId)

		})
	},
}
