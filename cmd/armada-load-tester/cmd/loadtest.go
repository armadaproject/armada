package cmd

import (
	"github.com/G-Research/k8s-batch/internal/client"
	"github.com/G-Research/k8s-batch/internal/client/domain"
	"github.com/G-Research/k8s-batch/internal/client/service"
	"github.com/G-Research/k8s-batch/internal/client/util"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	rootCmd.AddCommand(loadtestCmd)
	loadtestCmd.Flags().Bool("watchEvents", false, "If enabled, the program will watch the events of all submitted jobs before exiting")
	viper.BindPFlag("watchEvents", loadtestCmd.Flags().Lookup("watchEvents"))
}

var loadtestCmd = &cobra.Command{
	Use:   "loadtest ./path/to/loadtest/spec.yaml",
	Short: "Perform a load test of armada",
	Long: `Perform a load test of armada from a spec file.

	Example loadtest.yaml:

	submissions:
	  - name: example
		count: 5
		jobs:
		  - name: basic_job
			count: 10
			spec:
			  terminationGracePeriodSeconds: 0
			  restartPolicy: Never
			  containers:
				- name: sleep
				  imagePullPolicy: IfNotPresent
				  image: alpine:latest
				  command:
					- sh
				  args:
					- -c
					- sleep $(( (RANDOM % 60) + 100 ))
				  resources:
					limits:
					  memory: 128Mi
					  cpu: 80m
					requests:
					  memory: 64Mi
					  cpu: 60m

`,
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]
		loadTestSpec := &domain.LoadTestSpecification{}
		util.BindJsonOrYaml(filePath, loadTestSpec)

		watchEvents := viper.GetBool("watchEvents")
		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()
		loadTester := service.NewArmadaLoadTester(apiConnectionDetails)

		loadTester.RunSubmissionTest(*loadTestSpec)
	},
}
