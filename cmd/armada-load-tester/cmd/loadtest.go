package cmd

import (
	"github.com/G-Research/k8s-batch/internal/client"
	"github.com/G-Research/k8s-batch/internal/client/domain"
	"github.com/G-Research/k8s-batch/internal/client/service"
	"github.com/G-Research/k8s-batch/internal/client/util"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(loadtestCmd)
}

var loadtestCmd = &cobra.Command{
	Use:   "loadtest ./path/to/loadtest/spec.yaml",
	Short: "Perform a load test of armada using a spec file which defines all the jobs to submit",
	Long:  `Perform a load test of armada using a spec file which defines all the jobs to submit`,
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]
		loadTestSpec := &domain.LoadTestSpecification{}
		util.BindJsonOrYaml(filePath, loadTestSpec)

		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()
		loadTester := service.NewArmadaLoadTester(apiConnectionDetails)

		loadTester.RunSubmissionTest(*loadTestSpec)
	},
}
