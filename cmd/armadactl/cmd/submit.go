package cmd

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/client"
	"github.com/G-Research/armada/internal/client/service"
	"github.com/G-Research/armada/internal/client/util"
	"github.com/G-Research/armada/internal/client/validation"
)

func init() {
	rootCmd.AddCommand(submitCmd)
	submitCmd.Flags().Bool("dry-run", false, "Performs basic validation on the submitted file. Does no actual submission of jobs to the server.")
}

type JobSubmitFile struct {
	Jobs []*api.JobRequest `json:"jobs"`
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
		util.BindJsonOrYaml(filePath, submitFile)

		if dryRun {
			return
		}

		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()

		util.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
			client := api.NewSubmitClient(conn)
			for _, job := range submitFile.Jobs {
				response, e := service.SubmitJob(client, job)

				if e != nil {
					log.Error(e)
					os.Exit(1)
				}
				log.Infof("Submitted job id: %s (set: %s)", response.JobId, job.JobSetId)
			}
		})
	},
}
