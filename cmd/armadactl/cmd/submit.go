package cmd

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func init() {
	rootCmd.AddCommand(submitCmd)
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

		filePath := args[0]

		submitFile := &JobSubmitFile{}
		bindYaml(filePath, submitFile)

		withConnection(func(conn *grpc.ClientConn) {
			client := api.NewSubmitClient(conn)

			for _, job := range submitFile.Jobs {

				response, e := client.SubmitJob(timeout(), job)

				if e != nil {
					log.Error(e)
					break
				}
				log.Infof("Submitted job id: %s (set: %s)", response.JobId, job.JobSetId)
			}
		})
	},
}
