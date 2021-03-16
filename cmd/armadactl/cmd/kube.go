package cmd

import (
	"context"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

func init() {
	rootCmd.AddCommand(kubeCmd)

	kubeCmd.Flags().String(
		"jobId", "", "job to cancel")
	kubeCmd.MarkFlagRequired("jobId")
	kubeCmd.Flags().String(
		"queue", "", "queue of the job")
	kubeCmd.MarkFlagRequired("queue")
	kubeCmd.Flags().String(
		"jobSet", "", "jobSet of the job")
	kubeCmd.MarkFlagRequired("jobSet")
	kubeCmd.Flags().Int(
		"podNumber", 0, "[optional] for jobs with multiple pods, index of the pod")
	kubeCmd.FParseErrWhitelist.UnknownFlags = true
}

var kubeCmd = &cobra.Command{
	Use:   "kube",
	Short: "output kubectl command to access pod information",
	Long: `This command can be used to query kubernetes pods for a particular job.
Example:
	armadactl kube logs --queue my-queue --jobSet my-set --jobId 123456
	
In bash, you can execute it directly like this:
	$(armadactl kube logs --queue my-queue --jobSet my-set --jobId 123456) --tail=20
`,
	Run: func(cmd *cobra.Command, args []string) {
		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		jobId, _ := cmd.Flags().GetString("jobId")
		queue, _ := cmd.Flags().GetString("queue")
		jobSetId, _ := cmd.Flags().GetString("jobSet")
		podNumber, _ := cmd.Flags().GetInt("podNumber")

		verb := strings.Join(args, " ")

		client.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {

			eventsClient := api.NewEventClient(conn)
			state := client.GetJobSetState(eventsClient, queue, jobSetId, context.Background())
			jobInfo := state.GetJobInfo(jobId)

			if jobInfo == nil {
				log.Fatalf("Could not found job %s.", jobId)
			}

			if jobInfo.ClusterId == "" {
				log.Fatalf("The job have no cluster allocated.")
			}

			cmd := client.GetKubectlCommand(jobInfo.ClusterId, jobInfo.Job.Namespace, jobId, podNumber, verb)

			fmt.Println(cmd)
		})
	},
}
