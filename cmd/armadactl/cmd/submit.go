package cmd

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"github.com/G-Research/armada/pkg/client/util"
	"github.com/G-Research/armada/pkg/client/validation"
)

func init() {
	rootCmd.AddCommand(submitCmd)
	submitCmd.Flags().Bool("dry-run", false, "Performs basic validation on the submitted file. Does no actual submission of jobs to the server.")
}

type JobSubmitFile struct {
	Queue    string
	JobSetId string
	Jobs     []*api.JobSubmitRequestItem `json:"jobs"`
}

var submitCmd = &cobra.Command{
	Use:   "submit ./path/to/jobs.yaml",
	Short: "Submit jobs to armada",
	Long: `Submit jobs to armada from file.

	Example jobs.yaml:
	
	jobs:
	  - queue: test
		priority: 0
		jobSetId: set1
		podSpec:
		  ... kubernetes pod spec ...
`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dryRun, _ := cmd.Flags().GetBool("dry-run")
		filePath := args[0]

		ok, err := validation.ValidateSubmitFile(filePath)

		if !ok {
			log.Error(err)
			os.Exit(1)
		}

		submitFile := &JobSubmitFile{}
		err = util.BindJsonOrYaml(filePath, submitFile)

		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		if dryRun {
			return
		}

		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		requests := client.CreateChunkedSubmitRequests(submitFile.Queue, submitFile.JobSetId, submitFile.Jobs)

		util.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			submissionClient := api.NewSubmitClient(conn)
			for _, request := range requests {
				response, e := client.SubmitJobs(submissionClient, request)

				if e != nil {
					log.Error(e)
					os.Exit(1)
				}

				summariseResponse(response, request.JobSetId)
			}
		})
	},
}

func summariseResponse(response *api.JobSubmitResponse, jobSetId string) {
	for _, jobResponseItem := range response.JobResponseItems {
		if jobResponseItem.Error != "" {
			log.Errorf("Failed to submit job because: %s", jobResponseItem.Error)
		} else {
			log.Infof("Submitted job id: %s (set: %s)", jobResponseItem.JobId, jobSetId)
		}
	}
}
