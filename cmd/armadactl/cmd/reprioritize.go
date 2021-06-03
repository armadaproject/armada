package cmd

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

func init() {
	rootCmd.AddCommand(reprioritizeCmd)
	reprioritizeCmd.Flags().String(
		"jobId", "", "Job to reprioritize")
	reprioritizeCmd.Flags().String(
		"queue", "", "Queue including jobs to be reprioritized (requires job set to be specified)")
	reprioritizeCmd.Flags().String(
		"jobSet", "", "Job set including jobs to be reprioritized (requires queue to be specified)")
	reprioritizeCmd.Flags().Float64(
		"priority", 0, "New priority to assign to job(s)")
}

var reprioritizeCmd = &cobra.Command{
	Use:   "reprioritize",
	Short: "Reprioritize jobs in Armada",
	Long:  `Change the priority of a single or multiple jobs by specifying either a job id or a combination of queue & job set.`,
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		client.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			client := api.NewSubmitClient(conn)

			jobId, _ := cmd.Flags().GetString("jobId")
			queue, _ := cmd.Flags().GetString("queue")
			jobSet, _ := cmd.Flags().GetString("jobSet")
			priority, _ := cmd.Flags().GetFloat64("priority")
			var jobIds []string
			if jobId != "" {
				jobIds = append(jobIds, jobId)
			}

			ctx, cancel := common.ContextWithDefaultTimeout()
			defer cancel()
			result, err := client.ReprioritizeJobs(ctx, &api.JobReprioritizeRequest{
				JobIds:      jobIds,
				JobSetId:    jobSet,
				Queue:       queue,
				NewPriority: priority,
			})
			if err != nil {
				exitWithError(err)
			}
			if len(result.ReprioritizedIds) == 0 {
				exitWithError(fmt.Errorf("no jobs were reprioritized"))
			}
			log.Infof("Reprioritization request submitted for jobs: %s", strings.Join(result.ReprioritizedIds, ", "))
		})
	},
}
