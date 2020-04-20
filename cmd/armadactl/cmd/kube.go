package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

func init() {
	rootCmd.AddCommand(kubeCmd)

	kubeCmd.Flags().String(
		"jobId", "", "job to cancel")
	kubeCmd.MarkFlagRequired("jobId")
	kubeCmd.Flags().String(
		"queue", "", "queue to cancel jobs from (requires job set to be specified)")
	kubeCmd.MarkFlagRequired("queue")
	kubeCmd.Flags().String(
		"jobSet", "", "jobSet to cancel (requires queue to be specified)")
	kubeCmd.MarkFlagRequired("jobSet")
}

var kubeCmd = &cobra.Command{
	Use:   "kube",
	Short: "output kubectl command to access pod information",
	Long: `This command can be used to query kubernetes pods for a particular job.
Example:
	$(armadactl kube logs --queue my-queue --jobSet my-set --jobId 123456) -t 100
`,
	Run: func(cmd *cobra.Command, args []string) {
		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		jobId, _ := cmd.Flags().GetString("jobId")
		queue, _ := cmd.Flags().GetString("queue")
		jobSetId, _ := cmd.Flags().GetString("jobSet")

		verb := strings.Join(args, " ")

		client.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {

			eventsClient := api.NewEventClient(conn)
			state := client.GetJobSetState(eventsClient, queue, jobSetId, context.Background())
			jobInfo := state.GetJobInfo(jobId)

			cmd := client.GetKubectlCommand(jobInfo.ClusterId, jobInfo.Job.Namespace, verb+" "+common.PodNamePrefix+jobId)

			fmt.Println(cmd)
		})
	},
}
