package cmd

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/G-Research/armada/internal/client"
	"github.com/G-Research/armada/internal/client/domain"
	"github.com/G-Research/armada/internal/client/service"
	"github.com/G-Research/armada/internal/client/util"
)

func init() {
	rootCmd.AddCommand(loadtestCmd)
	loadtestCmd.Flags().Bool("watch", false, "If enabled, the program will watch the events of all submitted jobs before exiting")
	viper.BindPFlag("watch", loadtestCmd.Flags().Lookup("watch"))
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
		err := util.BindJsonOrYaml(filePath, loadTestSpec)

		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		watchEvents := viper.GetBool("watch")
		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()
		loadTester := service.NewArmadaLoadTester(apiConnectionDetails)

		loadTester.RunSubmissionTest(*loadTestSpec, watchEvents)
	},
}
