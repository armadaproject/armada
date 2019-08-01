package cmd

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/util/yaml"
	"os"
	"time"
)

// submitCmd represents the submit command
var submitCmd = &cobra.Command{
	Use:   "submit",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {

		filePath := args[0]
		submitFile := &JobSubmitFile{}

		yamlReader, err := os.Open(filePath)
		if err != nil {
			log.Printf("yamlFile.Get err   #%v ", err)
		}

		err = yaml.NewYAMLOrJSONDecoder(yamlReader, 128).Decode(submitFile)
		if err != nil {
			log.Fatalf("Unmarshal: %v", err)
		}

		url := cmd.Flag("armadaUrl").Value.String()
		conn, err := grpc.Dial(url, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.WaitForReady(false)))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		defer conn.Close()

		client := api.NewSubmitClient(conn)
		background := context.Background()

		for _, job := range submitFile.Jobs {

			ctx, _ := context.WithTimeout(background, 30*time.Second)
			response, e := client.SubmitJob(ctx, job)

			if e != nil {
				log.Error(e)
				break
			}
			log.Infof("Submitted job id: %s (set: %s)", response.JobId, job.JobSetId)
		}
	},
}

type JobSubmitFile struct {
	Jobs []*api.JobRequest `json:"jobs"`
}

func init() {
	rootCmd.AddCommand(submitCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// submitCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// submitCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
