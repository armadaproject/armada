package cmd

import (
	"context"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/domain"
	"github.com/armadaproject/armada/pkg/client/util"
)

const defaultTimeout = time.Second * -1

func init() {
	rootCmd.AddCommand(loadtestCmd)
	loadtestCmd.Flags().Bool("watch", false, "If enabled, the program will watch the events of all submitted jobs before exiting")
	loadtestCmd.Flags().Duration("timeout", defaultTimeout, "The duration the test will last for, before cancelling all remaining jobs")
	loadtestCmd.Flags().Bool("checkSuccess", false, "If enabled, the program will exit with -1 if any jobs submitted do not succeed")
	if err := viper.BindPFlag("watch", loadtestCmd.Flags().Lookup("watch")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("timeout", loadtestCmd.Flags().Lookup("timeout")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("checkSuccess", loadtestCmd.Flags().Lookup("checkSuccess")); err != nil {
		panic(err)
	}
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
				  image: alpine:3.18
				  command:
					- sh
				  args:
					- -c
					- sleep $(( (RANDOM % 60) + 100 ))
				  resources:
					limits:
					  memory: 64Mi
					  cpu: 60m
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
		timeout := viper.GetDuration("timeout")
		checkSuccess := viper.GetBool("checkSuccess")

		loadTestTimeout := context.Background()
		if timeout != defaultTimeout {
			loadTestTimeout, _ = context.WithTimeout(context.Background(), timeout)
		}
		apiConnectionDetails := client.ExtractCommandlineArmadaApiConnectionDetails()
		loadTester := client.NewArmadaLoadTester(apiConnectionDetails)
		result := loadTester.RunSubmissionTest(loadTestTimeout, *loadTestSpec, watchEvents)

		if watchEvents && checkSuccess {
			checkLoadTestSuccess(loadTestTimeout, result, loadTestSpec)
		}
	},
}

func checkLoadTestSuccess(loadTestDeadline context.Context, result domain.LoadTestSummary, loadTestSpec *domain.LoadTestSpecification) {
	if loadTestDeadline.Err() != nil {
		log.Error("Fail: Test timed out")
		os.Exit(1)
	}

	if len(result.SubmittedJobs) != loadTestSpec.NumberOfJobsInSpecification() {
		log.Error("Fail: Did not submit as many jobs as were in the test specification")
		os.Exit(1)
	}

	nonSuccessfulJobs := make([]string, 0, 10)
	for _, jobId := range result.SubmittedJobs {
		if result.CurrentState.GetJobInfo(jobId).Status != domain.Succeeded {
			nonSuccessfulJobs = append(nonSuccessfulJobs, jobId)
		}
	}
	if len(nonSuccessfulJobs) > 0 {
		log.Errorf("Fail: The following jobs have not completed successfully %s", strings.Join(nonSuccessfulJobs, ","))
		os.Exit(1)
	}
}
