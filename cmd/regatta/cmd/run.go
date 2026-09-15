package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"k8s.io/client-go/kubernetes"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/regatta/kwok"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/domain"
	"github.com/armadaproject/armada/pkg/client/util"
)

func init() {
	rootCmd.AddCommand(runCmd)
	runCmd.Flags().Bool("watch", true, "Watch submitted job events until the scenario completes")
	runCmd.Flags().Bool("teardownOnly", false, "Skip KWOK setup and job submission, just tear down any existing KWOK fake nodes/controller")
	runCmd.Flags().Bool("skipKwok", false, "Skip KWOK setup/teardown entirely and just submit the scenario against whatever cluster is already there")
	runCmd.Flags().String("kubeconfig", "", "Path to a kubeconfig file (defaults to KUBECONFIG env var, then $HOME/.kube/config)")
	runCmd.Flags().Int("kwokNodeCount", 20, "Number of KWOK fake nodes to create")
	runCmd.Flags().String("kindClusterName", "armada-test", "Name of the kind cluster the kwok-controller container should reach over the kind docker network")
	runCmd.Flags().Int("schedulableProbeRetries", 5, "Number of canary-job attempts to confirm the scheduler can actually place jobs on the KWOK fake nodes before running the scenario")
	runCmd.Flags().Duration("schedulableProbeDelay", 5*time.Second, "Delay before the first canary-job poll; doubles on each retry")
	if err := viper.BindPFlag("watch", runCmd.Flags().Lookup("watch")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("teardownOnly", runCmd.Flags().Lookup("teardownOnly")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("skipKwok", runCmd.Flags().Lookup("skipKwok")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("kubeconfig", runCmd.Flags().Lookup("kubeconfig")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("kwokNodeCount", runCmd.Flags().Lookup("kwokNodeCount")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("kindClusterName", runCmd.Flags().Lookup("kindClusterName")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("schedulableProbeRetries", runCmd.Flags().Lookup("schedulableProbeRetries")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("schedulableProbeDelay", runCmd.Flags().Lookup("schedulableProbeDelay")); err != nil {
		panic(err)
	}
}

var runCmd = &cobra.Command{
	Use:   "run ./path/to/scenario.yaml",
	Short: "Stand up KWOK fake nodes and run a benchmarking scenario against Armada",
	Long: `Stand up KWOK fake nodes and run a benchmarking scenario against Armada.

Scenario files use the same format as armada-load-tester. Jobs must both tolerate the
kwok.x-k8s.io/node=fake:NoSchedule taint AND select on kwok.x-k8s.io/node=fake applied to KWOK
fake nodes, or they will schedule onto real cluster nodes instead (a toleration alone only
permits scheduling onto a fake node, it doesn't require it - see
cmd/regatta/config/scenario.example.yaml):

	submissions:
	  - name: example
	    count: 5
	    jobs:
	      - name: basic_job
	        count: 10
	        spec:
	          terminationGracePeriodSeconds: 0
	          restartPolicy: Never
	          nodeSelector:
	            kwok.x-k8s.io/node: fake
	          tolerations:
	            - key: kwok.x-k8s.io/node
	              operator: Equal
	              value: fake
	              effect: NoSchedule
	          containers:
	            - name: sleep
	              imagePullPolicy: IfNotPresent
	              image: alpine:3.21.3
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
	Args: cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		teardownOnly := viper.GetBool("teardownOnly")
		skipKwok := viper.GetBool("skipKwok")
		if !teardownOnly && !skipKwok && len(args) != 1 {
			log.Error("a scenario file is required unless --teardownOnly is set")
			os.Exit(1)
		}

		ctx, cancel := context.WithCancel(context.Background())
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigCh
			log.Info("received interrupt, cancelling run")
			cancel()
		}()
		defer cancel()

		var kwokClient kubernetes.Interface
		if !skipKwok {
			var err error
			kwokClient, err = kwok.NewClientset(viper.GetString("kubeconfig"))
			if err != nil {
				log.Errorf("could not build kubernetes client: %s", err)
				os.Exit(1)
			}
		}

		if teardownOnly {
			if skipKwok {
				log.Info("--skipKwok set, nothing to tear down")
				return
			}
			if err := kwok.Teardown(ctx, kwokClient); err != nil {
				log.Errorf("KWOK teardown failed: %s", err)
				os.Exit(1)
			}
			log.Info("KWOK teardown complete")
			return
		}

		apiConnectionDetails, err := client.ExtractCommandlineArmadaApiConnectionDetails()
		if err != nil {
			log.Errorf("could not retrieve Armada API connection details: %s", err)
			os.Exit(1)
		}

		if !skipKwok {
			kwokCfg := kwok.Config{
				KubeconfigPath:       viper.GetString("kubeconfig"),
				KindClusterName:      viper.GetString("kindClusterName"),
				StageCRDPath:         "cmd/regatta/kwok/stage-crd.yaml",
				StagesPath:           "cmd/regatta/kwok/stages.yaml",
				NodeProfile:          kwok.GB200Slice,
				NodeCount:            viper.GetInt("kwokNodeCount"),
				ApiConnectionDetails: apiConnectionDetails,
				SchedulableProbe: kwok.ProbeConfig{
					Retries:      viper.GetInt("schedulableProbeRetries"),
					InitialDelay: viper.GetDuration("schedulableProbeDelay"),
				},
			}
			log.Info("setting up KWOK fake nodes")
			if err := kwok.Setup(ctx, kwokClient, kwokCfg); err != nil {
				log.Errorf("KWOK setup failed: %s", err)
				os.Exit(1)
			}
			defer func() {
				log.Info("tearing down KWOK fake nodes")
				if err := kwok.Teardown(context.Background(), kwokClient); err != nil {
					log.Errorf("KWOK teardown failed: %s", err)
				}
			}()
		}

		loadTestSpec := &domain.LoadTestSpecification{}
		if err := util.BindJsonOrYaml(args[0], loadTestSpec); err != nil {
			log.Error(err.Error())
			os.Exit(1)
		}

		watchEvents := viper.GetBool("watch")
		loadTester := client.NewArmadaLoadTester(apiConnectionDetails)
		result := loadTester.RunSubmissionTest(ctx, *loadTestSpec, watchEvents)
		log.Infof("submitted %d jobs", len(result.SubmittedJobs))
	},
}
